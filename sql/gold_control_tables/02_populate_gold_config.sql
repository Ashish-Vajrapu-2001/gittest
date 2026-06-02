-- Clear existing configurations for clean insert
DELETE FROM control.gold_dimension_config;
DELETE FROM control.gold_aggregation_rules;
DELETE FROM control.gold_table_config;

-- Insert Dimensions
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency, is_active)
VALUES 
('silver.customers,silver.customer_registration_source', 'dim_customer', 'DIMENSION', 'DAILY', 1),
('silver.products,silver.categories,silver.brands', 'dim_product', 'DIMENSION', 'DAILY', 1),
('silver.addresses,silver.city_tier_master', 'dim_geography', 'DIMENSION', 'DAILY', 1),
('silver.marketing_campaigns', 'dim_campaign', 'DIMENSION', 'DAILY', 1),
('none', 'dim_date', 'DIMENSION', 'DAILY', 1);

-- Insert Facts
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency, is_active)
VALUES 
('silver.orders', 'fact_orders', 'FACT', 'DAILY', 1),
('silver.order_lines', 'fact_order_lines', 'FACT', 'DAILY', 1),
('silver.marketing_campaigns', 'fact_campaign_performance', 'FACT', 'DAILY', 1),
('silver.surveys', 'fact_surveys', 'FACT', 'DAILY', 1);

-- Insert Aggregates
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency, is_active)
VALUES 
('fact_orders,fact_campaign_performance,dim_customer,dim_date,dim_campaign', 'agg_customer_clv', 'AGGREGATE', 'DAILY', 1),
('agg_customer_clv,dim_customer', 'agg_monthly_segment_metrics', 'AGGREGATE', 'DAILY', 1);

-- Configure Dimensions (SCD Types and Keys)
INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 2, 'customer_id', 'customer_type,status,marketing_opt_in'
FROM control.gold_table_config WHERE target_gold_table = 'dim_customer';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 1, 'product_id', NULL
FROM control.gold_table_config WHERE target_gold_table = 'dim_product';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 1, 'address_id', NULL
FROM control.gold_table_config WHERE target_gold_table = 'dim_geography';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 1, 'campaign_id', NULL
FROM control.gold_table_config WHERE target_gold_table = 'dim_campaign';

-- Configure Aggregation Rules (Using CUSTOM_SQL to trigger LLD specific logic in Databricks)
INSERT INTO control.gold_aggregation_rules (gold_table_id, aggregation_name, aggregation_type, target_column, is_active)
SELECT gold_table_id, 'Customer CLV Calculation', 'CUSTOM_SQL', 'clv', 1
FROM control.gold_table_config WHERE target_gold_table = 'agg_customer_clv';

INSERT INTO control.gold_aggregation_rules (gold_table_id, aggregation_name, aggregation_type, target_column, is_active)
SELECT gold_table_id, 'Monthly Segment Metrics', 'CUSTOM_SQL', 'avg_clv', 1
FROM control.gold_table_config WHERE target_gold_table = 'agg_monthly_segment_metrics';