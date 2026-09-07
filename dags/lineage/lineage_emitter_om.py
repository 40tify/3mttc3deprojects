import json
import os
import urllib.parse
import requests
from airflow.hooks.base import BaseHook

class OpenMetadataRestClient:
    """
    Lightweight, REST-native OpenMetadata client that avoids SDK version / Pydantic regex
    conflicts while providing 100% compatibility across Python 3.8-3.12.
    """
    def __init__(self, host="openmetadata-server:8585", jwt_token=None, schema="http"):
        self.base_url = f"{schema}://{host}/api/v1"
        self.jwt_token = jwt_token
        self.session = requests.Session()
        if self.jwt_token:
            self.session.headers.update({
                "Authorization": f"Bearer {self.jwt_token}",
                "Content-Type": "application/json",
            })

    def health_check(self) -> bool:
        """Check if OpenMetadata API is responding."""
        try:
            r = self.session.get(f"{self.base_url}/system/version", timeout=10)
            return r.status_code == 200
        except Exception as e:
            print(f"Health check failed against {self.base_url}: {e}")
            return False

    def get_entity_by_fqn(self, entity_type: str, fqn: str) -> dict:
        """
        Retrieves an entity (table, container, topic, dashboard, etc.) by its fully qualified name.
        """
        entity_endpoint_map = {
            "table": "tables",
            "container": "containers",
            "topic": "topics",
            "dashboard": "dashboards",
            "apiEndpoint": "apiEndpoints",
            "chart": "charts",
            "pipeline": "pipelines",
        }
        endpoint = entity_endpoint_map.get(entity_type, f"{entity_type}s")
        encoded_fqn = urllib.parse.quote(fqn, safe="")
        url = f"{self.base_url}/{endpoint}/name/{encoded_fqn}"
        
        response = self.session.get(url, timeout=15)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            response.raise_for_status()

    def add_lineage(self, source_id: str, source_type: str, target_id: str, target_type: str, column_lineage: list = None) -> dict:
        """
        Emits an edge into the OpenMetadata lineage graph via PUT /api/v1/lineage.
        """
        edge_payload = {
            "fromEntity": {
                "id": source_id,
                "type": source_type
            },
            "toEntity": {
                "id": target_id,
                "type": target_type
            }
        }
        if column_lineage:
            edge_payload["lineageDetails"] = {
                "columnsLineage": column_lineage
            }

        url = f"{self.base_url}/lineage"
        response = self.session.put(url, json={"edge": edge_payload}, timeout=15)
        if response.status_code not in (200, 201):
            print(f"OpenMetadata Lineage API Error ({response.status_code}): {response.text}")
            response.raise_for_status()
        if response.text and response.text.strip():
            try:
                return response.json()
            except Exception:
                return {"status": "success", "text": response.text}
        return {"status": "success", "statusCode": response.status_code}

def get_openmetadata_client() -> OpenMetadataRestClient:
    """
    Initializes and returns an authenticated OpenMetadata REST client using
    the Airflow connection 'openmetadata_default'.
    """
    try:
        conn = BaseHook.get_connection("openmetadata_default")
        schema = conn.schema or "http"
        host = conn.host or "openmetadata-server:8585"
        token = conn.password
    except Exception as e:
        print(f"Airflow connection 'openmetadata_default' not found ({e}). Falling back to environment defaults.")
        schema = os.getenv("OPENMETADATA_SCHEMA", "http")
        host = os.getenv("OPENMETADATA_HOST", "openmetadata-server:8585")
        token = os.getenv("OPENMETADATA_JWT_TOKEN", "")

    client = OpenMetadataRestClient(host=host, jwt_token=token, schema=schema)
    return client

def emit_lineage_to_om(
    source_fqn: str,
    target_fqn: str,
    source_type: str,
    target_type: str,
    column_mappings: list,
):
    """
    Emits data lineage (table/container-level and column-level) to OpenMetadata.
    """
    client = get_openmetadata_client()

    print(f"Resolving entities in OpenMetadata: Source={source_fqn} ({source_type}), Target={target_fqn} ({target_type})")
    source_entity = client.get_entity_by_fqn(entity_type=source_type, fqn=source_fqn)
    target_entity = client.get_entity_by_fqn(entity_type=target_type, fqn=target_fqn)

    if not source_entity:
        raise ValueError(
            f"Source entity not found in OpenMetadata: '{source_fqn}' (type: {source_type}). "
            "Please ensure metadata ingestion has run or the entity exists."
        )
    if not target_entity:
        raise ValueError(
            f"Target entity not found in OpenMetadata: '{target_fqn}' (type: {target_type}). "
            "Please ensure metadata ingestion has run or the entity exists."
        )

    # OpenMetadata 1.2.4 validates that column-level lineage is only emitted between tables
    both_support_columns = (source_type == "table" and target_type == "table")

    column_lineage_list = []
    if both_support_columns and column_mappings:
        for mapping in column_mappings:
            if isinstance(mapping, (tuple, list)) and len(mapping) == 2:
                src_col, tgt_col = mapping
            elif isinstance(mapping, str):
                src_col = tgt_col = mapping
            elif isinstance(mapping, dict):
                src_col = mapping.get("source")
                tgt_col = mapping.get("target")
            else:
                raise ValueError(f"Invalid column mapping format: {mapping}")

            column_lineage_list.append({
                "fromColumns": [f"{source_fqn}.{src_col}"],
                "toColumn": f"{target_fqn}.{tgt_col}"
            })

    result = client.add_lineage(
        source_id=source_entity["id"],
        source_type=source_type,
        target_id=target_entity["id"],
        target_type=target_type,
        column_lineage=column_lineage_list if column_lineage_list else None
    )

    if column_lineage_list:
        print(f"Column-level lineage emitted successfully: {source_fqn} -> {target_fqn} ({len(column_lineage_list)} columns).")
    else:
        print(f"Entity-level lineage emitted successfully: {source_fqn} -> {target_fqn}.")
    return result
