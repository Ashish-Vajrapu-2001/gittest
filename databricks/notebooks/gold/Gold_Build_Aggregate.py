# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Build Aggregate
# MAGIC Pre-computes complex CLV metrics and reporting aggregates based on LLD

# COMMAND ----------

# 1. Get Parameters
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

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

# 3. Register Gold Tables as Views
gold_base = f"abfss://datalake@{storage_account}.dfs.core.windows.net/gold"

spark.read.format("delta").load(f"{gold_base}/dimensions/dim_customer/").createOrReplaceTempView("dim_customer")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_campaign/").createOrReplaceTempView("dim_campaign")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_date/").createOrReplaceTempView("dim_date")
spark.read.format("delta").load(f"{gold_base}/facts/fact_orders/").createOrReplaceTempView("fact_orders")
spark.read.format("delta").load(f"{gold_base}/facts/fact_campaign_performance/").createOrReplaceTempView("fact_campaign_performance")

if target_gold_table == 'agg_monthly_segment_metrics':
    spark.read.format("delta").load(f"{gold_base}/aggregates/agg_customer_clv/").createOrReplaceTempView("agg_customer_clv")

# COMMAND ----------

# 4. Execute Aggregation SQL (Strictly from LLD)
if target_gold_table == 'agg_customer_clv':
    sql_query = """
    WITH customer_base AS (
        SELECT 
            c.dim_customer_key,
            c.registration_date,
            c.acquisition_channel,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.total_amount) AS total_revenue,
            MIN(d.full_date) AS first_order_date,
            MAX(d.full_date) AS last_order_date,
            DATEDIFF(DAY, MAX(d.full_date), CURRENT_DATE()) AS days_since_last_order
        FROM dim_customer c
        LEFT JOIN fact_orders o ON c.dim_customer_key = o.dim_customer_key
        LEFT JOIN dim_date d ON o.dim_date_key = d.dim_date_key
        WHERE c._is_current = true
        GROUP BY c.dim_customer_key, c.registration_date, c.acquisition_channel
    ),
    channel_cac AS (
        SELECT 
            c.channel,
            SUM(f.total_spend) / NULLIF(SUM(f.customers_acquired), 0) AS cac
        FROM fact_campaign_performance f
        JOIN dim_campaign c ON f.dim_campaign_key = c.dim_campaign_key
        GROUP BY c.channel
    )
    SELECT 
        cb.dim_customer_key,
        cb.total_orders,
        COALESCE(cb.total_revenue, 0) AS total_revenue,
        cb.first_order_date,
        cb.last_order_date,
        
        -- AOV
        CAST(CASE WHEN cb.total_orders > 0 THEN cb.total_revenue / cb.total_orders ELSE 0 END AS DECIMAL(15,2)) AS aov,
        
        -- Lifespan (Months)
        CAST(DATEDIFF(MONTH, cb.registration_date, 
            CASE WHEN cb.days_since_last_order <= 90 THEN CURRENT_DATE() ELSE cb.last_order_date END
        ) AS INT) AS lifespan_months,
        
        -- Purchase Frequency
        CAST(CASE WHEN DATEDIFF(MONTH, cb.registration_date, cb.last_order_date) > 0 
             THEN cb.total_orders / DATEDIFF(MONTH, cb.registration_date, cb.last_order_date)
             ELSE cb.total_orders 
        END AS DECIMAL(10,4)) AS purchase_frequency,
        
        -- CAC
        CAST(COALESCE(cc.cac, 0) AS DECIMAL(15,2)) AS allocated_cac,
        
        -- CLV
        CAST(CASE WHEN cb.total_orders = 0 THEN 0
             ELSE (
                (cb.total_revenue / cb.total_orders) * 
                (cb.total_orders / NULLIF(DATEDIFF(MONTH, cb.registration_date, cb.last_order_date), 0)) * 
                DATEDIFF(MONTH, cb.registration_date, CASE WHEN cb.days_since_last_order <= 90 THEN CURRENT_DATE() ELSE cb.last_order_date END)
             ) - COALESCE(cc.cac, 0)
        END AS DECIMAL(15,2)) AS clv,
        
        -- Loyalty Tier
        CASE 
            WHEN cb.total_revenue >= 10000 THEN 'Platinum'
            WHEN cb.total_revenue >= 5000 THEN 'Gold'
            WHEN cb.total_revenue >= 1000 THEN 'Silver'
            ELSE 'Bronze'
        END AS loyalty_tier,
        
        CURRENT_TIMESTAMP() AS _created_date
    FROM customer_base cb
    LEFT JOIN channel_cac cc ON cb.acquisition_channel = cc.channel
    """
    zorder_col = "dim_customer_key"

elif target_gold_table == 'agg_monthly_segment_metrics':
    sql_query = """
    SELECT 
        DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM') AS report_month,
        a.loyalty_tier,
        c.acquisition_channel,
        COUNT(a.dim_customer_key) AS total_customers,
        CAST(SUM(a.total_revenue) AS DECIMAL(15,2)) AS segment_revenue,
        CAST(AVG(a.clv) AS DECIMAL(15,2)) AS avg_clv,
        CAST(AVG(a.aov) AS DECIMAL(15,2)) AS avg_aov,
        CAST(AVG(a.allocated_cac) AS DECIMAL(15,2)) AS avg_cac,
        CURRENT_TIMESTAMP() AS _created_date
    FROM agg_customer_clv a
    JOIN dim_customer c ON a.dim_customer_key = c.dim_customer_key AND c._is_current = true
    GROUP BY a.loyalty_tier, c.acquisition_channel
    """
    zorder_col = "loyalty_tier"

# COMMAND ----------

# 5. Execute and Write to Gold
df_agg = spark.sql(sql_query)
agg_path = f"{gold_base}/aggregates/{target_gold_table}/"

df_agg.write.format("delta").mode("overwrite").save(agg_path)

# COMMAND ----------

# 6. Optimize
spark.sql(f"OPTIMIZE delta.`{agg_path}` ZORDER BY ({zorder_col})")
records_processed = df_agg.count()
dbutils.notebook.exit(str(records_processed))