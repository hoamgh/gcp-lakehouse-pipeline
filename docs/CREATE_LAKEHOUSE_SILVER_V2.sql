-- Execute only after the silver_v2 Delta paths have been populated and audited.
CREATE SCHEMA IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2`
OPTIONS(location = 'asia-southeast1');

CREATE EXTERNAL TABLE IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2.silver_customers` OPTIONS(format = 'DELTA_LAKE', uris = ['gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/silver_customers']);
CREATE EXTERNAL TABLE IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2.silver_products` OPTIONS(format = 'DELTA_LAKE', uris = ['gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/silver_products']);
CREATE EXTERNAL TABLE IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2.silver_sellers` OPTIONS(format = 'DELTA_LAKE', uris = ['gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/silver_sellers']);
CREATE EXTERNAL TABLE IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2.silver_orders` OPTIONS(format = 'DELTA_LAKE', uris = ['gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/silver_orders']);
CREATE EXTERNAL TABLE IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2.silver_order_items` OPTIONS(format = 'DELTA_LAKE', uris = ['gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/silver_order_items']);
CREATE EXTERNAL TABLE IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2.silver_payments` OPTIONS(format = 'DELTA_LAKE', uris = ['gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/silver_payments']);
CREATE EXTERNAL TABLE IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2.silver_reviews` OPTIONS(format = 'DELTA_LAKE', uris = ['gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/silver_reviews']);
CREATE EXTERNAL TABLE IF NOT EXISTS `hybrid-elt-lakehouse-pipeline.lakehouse_silver_v2.silver_shipments` OPTIONS(format = 'DELTA_LAKE', uris = ['gs://hybrid-elt-lakehouse-pipeline-lakehouse/silver_v2/silver_shipments']);
