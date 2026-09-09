FROM apache/airflow:2.9.3

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

RUN pip install \
    apache-airflow-providers-google==10.19.0 \
    dbt-bigquery==1.12.0

COPY --chown=airflow:root dbt_transform /opt/airflow/dbt_transform
RUN cp /opt/airflow/dbt_transform/profiles.yml.example /opt/airflow/dbt_transform/profiles.yml
