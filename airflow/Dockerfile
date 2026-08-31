FROM apache/airflow:2.9.3

# Tắt warning thông báo nâng cấp pip
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

# Cài đặt các thư viện cần thiết phục vụ GCP và dbt
RUN pip install apache-airflow-providers-google \
    dbt-bigquery \
    dbt-core
