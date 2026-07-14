FROM apache/airflow:2.7.1-python3.11

# Switch to airflow user to install packages
USER airflow

# Install pandas, Faker, and the Google Cloud provider for Airflow
RUN pip install --no-cache-dir \
    Faker==40.28.1 \
    pandas==2.1.1 \
    boto3==1.28.61 \
    apache-airflow-providers-google==10.8.0
