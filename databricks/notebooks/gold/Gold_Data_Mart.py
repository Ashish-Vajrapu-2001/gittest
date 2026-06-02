# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Data Mart Views
# MAGIC Creates denormalized views for BI tools (Power BI) consumption

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure Unity Catalog schema exists
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC USE gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create View: v_Customer_CLV_Dashboard
# MAGIC CREATE OR REPLACE VIEW gold.v_Customer_CLV_Dashboard AS
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.email,
# MAGIC     c.customer_type,
# MAGIC     c.acquisition_channel,
# MAGIC     a.loyalty_tier,
# MAGIC     a.total_orders,
# MAGIC     a.total_revenue,
# MAGIC     a.aov,
# MAGIC     a.purchase_frequency,
# MAGIC     a.lifespan_months,
# MAGIC     a.allocated_cac,
# MAGIC     a.clv,
# MAGIC     a.first_order_date,
# MAGIC     a.last_order_date
# MAGIC FROM delta.`abfss://datalake@{{PLACEHOLDER_STORAGE_ACCOUNT}}.dfs.core.windows.net/gold/aggregates/agg_customer_clv/` a
# MAGIC JOIN delta.`abfss://datalake@{{PLACEHOLDER_STORAGE_ACCOUNT}}.dfs.core.windows.net/gold/dimensions/dim_customer/` c 
# MAGIC   ON a.dim_customer_key = c.dim_customer_key
# MAGIC WHERE c._is_current = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create View: v_Sales_Performance
# MAGIC CREATE OR REPLACE VIEW gold.v_Sales_Performance AS
# MAGIC SELECT 
# MAGIC     d.full_date,
# MAGIC     d.year,
# MAGIC     d.month_name,
# MAGIC     g.country,
# MAGIC     g.state,
# MAGIC     g.city_tier,
# MAGIC     c.customer_type,
# MAGIC     f.order_number,
# MAGIC     f.total_amount,
# MAGIC     f.discount_amount
# MAGIC FROM delta.`abfss://datalake@{{PLACEHOLDER_STORAGE_ACCOUNT}}.dfs.core.windows.net/gold/facts/fact_orders/` f
# MAGIC JOIN delta.`abfss://datalake@{{PLACEHOLDER_STORAGE_ACCOUNT}}.dfs.core.windows.net/gold/dimensions/dim_date/` d ON f.dim_date_key = d.dim_date_key
# MAGIC JOIN delta.`abfss://datalake@{{PLACEHOLDER_STORAGE_ACCOUNT}}.dfs.core.windows.net/gold/dimensions/dim_geography/` g ON f.dim_geography_key = g.dim_geography_key
# MAGIC JOIN delta.`abfss://datalake@{{PLACEHOLDER_STORAGE_ACCOUNT}}.dfs.core.windows.net/gold/dimensions/dim_customer/` c ON f.dim_customer_key = c.dim_customer_key;