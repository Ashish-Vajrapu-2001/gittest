# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Build Fact
# MAGIC Processes Silver transactions into Gold Facts with Dimension Lookups

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# 1. Get Parameters
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("source_silver_tables", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_gold_table = dbutils.widgets.get("target_gold_table")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------

# 2. Storage authentication
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

# 3. Define Paths and Register Views
gold_base = f"abfss://datalake@{storage_account}.dfs.core.windows.net/gold"
silver_base = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver"

# Register Dimensions
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_customer/").createOrReplaceTempView("dim_customer")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_product/").createOrReplaceTempView("dim_product")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_geography/").createOrReplaceTempView("dim_geography")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_campaign/").createOrReplaceTempView("dim_campaign")
# Assuming dim_date is populated
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_date/").createOrReplaceTempView("dim_date")

# COMMAND ----------

# 4. Execute Fact Specific SQL Logic (from LLD)
if target_gold_table == 'fact_orders':
    spark.read.format("delta").load(f"{silver_base}/orders/").createOrReplaceTempView("silver_orders")
    
    sql_query = f"""
    SELECT 
        CAST(DATE_FORMAT(o.order_date, 'yyyyMMdd') AS INT) AS dim_date_key,
        COALESCE(c.dim_customer_key, -1) AS dim_customer_key,
        COALESCE(g.dim_geography_key, -1) AS dim_geography_key,
        o.order_id,
        o.order_number,
        o.order_status,
        CAST(o.subtotal_amount AS DECIMAL(15,2)) AS subtotal_amount,
        CAST(o.discount_amount AS DECIMAL(15,2)) AS discount_amount,
        CAST(o.tax_amount AS DECIMAL(15,2)) AS tax_amount,
        CAST(o.shipping_amount AS DECIMAL(15,2)) AS shipping_amount,
        CAST(o.total_amount AS DECIMAL(15,2)) AS total_amount,
        CURRENT_TIMESTAMP() AS _created_date,
        CURRENT_TIMESTAMP() AS _last_modified_date,
        '{pipeline_run_id}' AS _pipeline_run_id
    FROM silver_orders o
    LEFT JOIN dim_customer c ON o.customer_id = c.customer_id AND c._is_current = true
    LEFT JOIN dim_geography g ON o.shipping_address_id = g.address_id
    """
    zorder_col = "dim_customer_key"
    partition_col = "dim_date_key"

elif target_gold_table == 'fact_order_lines':
    spark.read.format("delta").load(f"{silver_base}/order_lines/").createOrReplaceTempView("silver_order_lines")
    spark.read.format("delta").load(f"{silver_base}/orders/").createOrReplaceTempView("silver_orders")
    
    sql_query = f"""
    SELECT 
        CAST(DATE_FORMAT(o.order_date, 'yyyyMMdd') AS INT) AS dim_date_key,
        COALESCE(c.dim_customer_key, -1) AS dim_customer_key,
        COALESCE(p.dim_product_key, -1) AS dim_product_key,
        COALESCE(g.dim_geography_key, -1) AS dim_geography_key,
        l.line_id,
        l.order_id,
        CAST(l.quantity AS INT) AS quantity,
        CAST(l.line_amount AS DECIMAL(15,2)) AS line_amount,
        CAST(l.unit_price AS DECIMAL(15,2)) AS unit_price,
        CURRENT_TIMESTAMP() AS _created_date,
        '{pipeline_run_id}' AS _pipeline_run_id
    FROM silver_order_lines l
    JOIN silver_orders o ON l.order_id = o.order_id
    LEFT JOIN dim_customer c ON o.customer_id = c.customer_id AND c._is_current = true
    LEFT JOIN dim_product p ON l.product_id = p.product_id
    LEFT JOIN dim_geography g ON o.shipping_address_id = g.address_id
    """
    zorder_col = "dim_product_key"
    partition_col = "dim_date_key"

elif target_gold_table == 'fact_campaign_performance':
    spark.read.format("delta").load(f"{silver_base}/marketing_campaigns/").createOrReplaceTempView("silver_campaigns")
    
    sql_query = f"""
    SELECT 
        CAST(DATE_FORMAT(m.start_date, 'yyyyMMdd') AS INT) AS dim_date_key,
        COALESCE(c.dim_campaign_key, -1) AS dim_campaign_key,
        CAST(m.total_spend AS DECIMAL(15,2)) AS total_spend,
        CAST(m.customers_acquired AS INT) AS customers_acquired,
        CURRENT_TIMESTAMP() AS _created_date,
        '{pipeline_run_id}' AS _pipeline_run_id
    FROM silver_campaigns m
    LEFT JOIN dim_campaign c ON m.campaign_id = c.campaign_id
    """
    zorder_col = "dim_campaign_key"
    partition_col = None

elif target_gold_table == 'fact_surveys':
    spark.read.format("delta").load(f"{silver_base}/surveys/").createOrReplaceTempView("silver_surveys")
    
    sql_query = f"""
    SELECT 
        CAST(DATE_FORMAT(s.response_date, 'yyyyMMdd') AS INT) AS dim_date_key,
        COALESCE(c.dim_customer_key, -1) AS dim_customer_key,
        s.survey_id,
        s.survey_type,
        CASE 
            WHEN s.nps_score BETWEEN 0 AND 6 THEN 'Detractor'
            WHEN s.nps_score BETWEEN 7 AND 8 THEN 'Passive'
            WHEN s.nps_score BETWEEN 9 AND 10 THEN 'Promoter'
            ELSE 'Unknown'
        END AS nps_category,
        CAST(s.nps_score AS INT) AS nps_score,
        CAST(s.csat_score AS INT) AS csat_score,
        CURRENT_TIMESTAMP() AS _created_date,
        '{pipeline_run_id}' AS _pipeline_run_id
    FROM silver_surveys s
    LEFT JOIN dim_customer c ON s.customer_id = c.customer_id AND c._is_current = true
    """
    zorder_col = "dim_customer_key"
    partition_col = None

# COMMAND ----------

# 5. Execute and Write to Gold
df_fact = spark.sql(sql_query)
fact_path = f"{gold_base}/facts/{target_gold_table}/"

writer = df_fact.write.format("delta").mode("overwrite")
if partition_col:
    writer = writer.partitionBy(partition_col)
    
writer.save(fact_path)

# COMMAND ----------

# 6. Optimize
spark.sql(f"OPTIMIZE delta.`{fact_path}` ZORDER BY ({zorder_col})")
records_processed = df_fact.count()
dbutils.notebook.exit(str(records_processed))