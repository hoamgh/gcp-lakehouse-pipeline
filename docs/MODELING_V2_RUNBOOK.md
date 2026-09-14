# Modeling v2 migration runbook

These commands are prepared but have not been executed. All destinations are
isolated from production.

## 1. Publish versioned Spark artifacts

```powershell
$project = 'hybrid-elt-lakehouse-pipeline'
$region = 'asia-southeast1'
$bucket = 'hybrid-elt-lakehouse-pipeline-lakehouse'
$serviceAccount = 'elt-pipeline-sa@hybrid-elt-lakehouse-pipeline.iam.gserviceaccount.com'
$subnet = 'projects/hybrid-elt-lakehouse-pipeline/regions/asia-southeast1/subnetworks/default'
gcloud storage cp spark_jobs/stream_bronze_to_silver.py "gs://$bucket/scripts/modeling_v2/stream_bronze_to_silver.py"
gcloud storage cp spark_jobs/config.py "gs://$bucket/scripts/modeling_v2/config.py"
gcloud storage cp spark_jobs/logger.py "gs://$bucket/scripts/modeling_v2/logger.py"
```

## 2. Populate isolated Silver

```powershell
gcloud dataproc batches submit pyspark "gs://$bucket/scripts/modeling_v2/stream_bronze_to_silver.py" `
  --batch=modeling-v2-bronze-to-silver `
  --project=$project --region=$region --service-account=$serviceAccount --subnet=$subnet `
  --py-files="gs://$bucket/scripts/modeling_v2/config.py,gs://$bucket/scripts/modeling_v2/logger.py" `
  --properties="spark.jars.packages=io.delta:delta-spark_2.13:3.2.1,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,spark.dataproc.driverEnv.PIPELINE_ENV=gcs,spark.dataproc.driverEnv.GCS_BUCKET=$bucket,spark.dataproc.driverEnv.SILVER_GCS_PREFIX=silver_v2,spark.executorEnv.PIPELINE_ENV=gcs,spark.executorEnv.GCS_BUCKET=$bucket,spark.executorEnv.SILVER_GCS_PREFIX=silver_v2"
```

This writes only to `gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/`,
including new checkpoints under `silver_v2/_checkpoints/`.

## 3. Register isolated BigQuery external tables

```powershell
Get-Content docs/CREATE_LAKEHOUSE_SILVER_V2.sql -Raw |
  bq query --project_id=$project --location=$region --use_legacy_sql=false
```

## 4. Build isolated analytics

```powershell
cd dbt_transform
$env:PROJECT_ID = $project
$env:REGION = $region
$env:SILVER_DATASET = 'lakehouse_silver_v2'
$env:ANALYTICS_V2_DATASET = 'analytics_v2'
dbt parse --profiles-dir profiles_v2 --target v2
dbt compile --profiles-dir profiles_v2 --target v2
# dbt Fusion incremental unit tests require the model-under-test relation to
# exist on a brand-new target. Bootstrap only this isolated v2 table first.
dbt run --profiles-dir profiles_v2 --target v2 --select fact_orders
dbt build --profiles-dir profiles_v2 --target v2
cd ..
```

## 5. Validate without cutover

Replace `PROJECT_ID` in a reviewed copy of the validation SQL, then run:

```powershell
(Get-Content docs/VALIDATE_MODELING_V2.sql -Raw).Replace('PROJECT_ID', $project) |
  bq query --project_id=$project --location=$region --use_legacy_sql=false
```

Do not cut over until all tests pass, the five-column grain is unique, legacy
`order_item_sk` values match production, and every row-count delta is explained.
