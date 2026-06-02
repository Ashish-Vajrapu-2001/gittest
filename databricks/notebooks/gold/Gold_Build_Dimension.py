# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Build Dimension
# MAGIC Processes Silver tables into Gold Dimensions (SCD1 and SCD2)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# 1. Get Parameters
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("source_silver_tables", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("scd_type", "1")
dbutils.widgets.text("business_key_columns", "")
dbutils.widgets.text("tracked_columns", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_gold_table = dbutils.widgets.get("target_gold_table")
scd_type = int(dbutils.widgets.get("scd_type"))
business_key = dbutils.widgets.get("business_key_columns")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------

# 2. Storage authentication - REQUIRED for ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

# 3. Define Paths
gold_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/gold/dimensions/{target_gold_table}/"
silver_base_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/"

# COMMAND ----------

# 4. Read and Transform Source Data based on LLD specifications
if target_gold_table == 'dim_customer':
    df_cust = spark.read.format("delta").load(f"{silver_base_path}customers/")
    df_reg = spark.read.format("delta").load(f"{silver_base_path}customer_registration_source/")
    
    df_source = df_cust.alias("c").join(
        df_reg.alias("r"), 
        col("c.customer_id") == col("r.customer_id"), 
        "left"
    ).select(
        col("c.customer_id"),
        col("c.email"),
        col("c.first_name"),
        col("c.last_name"),
        col("c.gender"),
        col("c.date_of_birth"),
        col("c.registration_date"),
        col("c.customer_type"),
        col("c.status"),
        col("c.marketing_opt_in"),
        col("c.preferred_language"),
        coalesce(col("r.channel"), lit("UNKNOWN")).alias("acquisition_channel")
    )

elif target_gold_table == 'dim_product':
    df_prod = spark.read.format("delta").load(f"{silver_base_path}products/")
    df_cat = spark.read.format("delta").load(f"{silver_base_path}categories/")
    df_brand = spark.read.format("delta").load(f"{silver_base_path}brands/")
    
    df_source = df_prod.alias("p").join(
        df_cat.alias("c"), col("p.category_id") == col("c.category_id"), "left"
    ).join(
        df_brand.alias("b"), col("p.brand_id") == col("b.brand_id"), "left"
    ).select(
        col("p.inventory_item_id").alias("product_id"),
        col("p.sku"),
        col("p.item_name").alias("product_name"),
        coalesce(col("c.category_name"), lit("UNKNOWN")).alias("category_name"),
        coalesce(col("b.brand_name"), lit("UNKNOWN")).alias("brand_name"),
        col("p.cost").alias("unit_cost")
    )

elif target_gold_table == 'dim_geography':
    df_addr = spark.read.format("delta").load(f"{silver_base_path}addresses/")
    df_tier = spark.read.format("delta").load(f"{silver_base_path}city_tier_master/")
    
    df_source = df_addr.alias("a").join(
        df_tier.alias("t"), 
        (col("a.city") == col("t.city")) & (col("a.state") == col("t.state")), 
        "left"
    ).select(
        col("a.address_id"),
        col("a.city"),
        col("a.state"),
        col("a.country"),
        col("a.pincode").alias("zip_code"),
        coalesce(col("t.tier").cast("string"), lit("UNCLASSIFIED")).alias("city_tier")
    )

elif target_gold_table == 'dim_campaign':
    df_camp = spark.read.format("delta").load(f"{silver_base_path}marketing_campaigns/")
    df_source = df_camp.select(
        col("campaign_id"),
        col("campaign_name"),
        col("channel"),
        col("start_date")
    )
else:
    dbutils.notebook.exit("0") # dim_date is pre-populated

# Add audit columns
df_source = df_source.withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
                     .withColumn("_last_modified_date", current_timestamp())

# COMMAND ----------

# 5. Handle Initial Load & Unknown Member
if not DeltaTable.isDeltaTable(spark, gold_path):
    # Create empty dataframe with schema
    sk_col = f"{target_gold_table}_key"
    
    if scd_type == 2:
        df_init = df_source.withColumn(sk_col, lit(-1).cast("bigint")) \
                           .withColumn("_valid_from", current_timestamp()) \
                           .withColumn("_valid_to", lit(None).cast("timestamp")) \
                           .withColumn("_is_current", lit(True)) \
                           .withColumn("_version", lit(1)) \
                           .withColumn("_created_date", current_timestamp())
    else:
        df_init = df_source.withColumn(sk_col, lit(-1).cast("bigint")) \
                           .withColumn("_created_date", current_timestamp())
                           
    # Create Unknown Member Record
    unknown_row = df_init.limit(1).withColumn(business_key, lit(-1))
    for c in unknown_row.columns:
        if dict(unknown_row.dtypes)[c] == 'string' and c not in ['_pipeline_run_id']:
            unknown_row = unknown_row.withColumn(c, lit("UNKNOWN"))
            
    unknown_row.write.format("delta").mode("overwrite").save(gold_path)

# COMMAND ----------

# 6. Perform Merge (SCD1 or SCD2)
gold_table = DeltaTable.forPath(spark, gold_path)
sk_col = f"{target_gold_table}_key"

if scd_type == 1:
    # SCD Type 1 - Overwrite
    # Generate SK for new records
    max_sk = spark.read.format("delta").load(gold_path).agg(max(sk_col)).collect()[0][0]
    if max_sk is None: max_sk = 0
    
    # Identify new vs existing
    existing_keys = spark.read.format("delta").load(gold_path).select(business_key)
    df_new = df_source.join(existing_keys, business_key, "left_anti")
    
    # Add SK to new records
    from pyspark.sql.window import Window
    w = Window.orderBy(business_key)
    df_new_with_sk = df_new.withColumn(sk_col, row_number().over(w) + lit(max_sk)) \
                           .withColumn("_created_date", current_timestamp())
                           
    df_existing_updates = df_source.join(existing_keys, business_key, "inner")
    
    # Union and Merge
    df_upsert = df_new_with_sk.unionByName(df_existing_updates, allowMissingColumns=True)
    
    gold_table.alias("t").merge(
        df_upsert.alias("s"),
        f"t.{business_key} = s.{business_key}"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

elif scd_type == 2:
    # SCD Type 2 - History Tracking (dim_customer)
    tracked_cols = dbutils.widgets.get("tracked_columns").split(",")
    
    # Get current records
    df_current = spark.read.format("delta").load(gold_path).filter(col("_is_current") == True)
    
    # Identify changes
    cond = " OR ".join([f"s.{c} != t.{c}" for c in tracked_cols])
    
    # Records to update (close old version)
    df_updates = df_source.alias("s").join(
        df_current.alias("t"),
        col(f"s.{business_key}") == col(f"t.{business_key}")
    ).filter(expr(cond)).selectExpr("s.*")
    
    # Records to insert (new versions + brand new records)
    existing_keys = df_current.select(business_key)
    df_new = df_source.join(existing_keys, business_key, "left_anti")
    df_inserts = df_new.unionByName(df_updates)
    
    # Generate SKs for inserts
    max_sk = spark.read.format("delta").load(gold_path).agg(max(sk_col)).collect()[0][0]
    if max_sk is None: max_sk = 0
    
    w = Window.orderBy(business_key)
    df_inserts_sk = df_inserts.withColumn(sk_col, row_number().over(w) + lit(max_sk)) \
                              .withColumn("_valid_from", current_timestamp()) \
                              .withColumn("_valid_to", lit(None).cast("timestamp")) \
                              .withColumn("_is_current", lit(True)) \
                              .withColumn("_version", lit(1)) \
                              .withColumn("_created_date", current_timestamp())
                              
    # Execute Merge for closing old records
    if df_updates.count() > 0:
        gold_table.alias("t").merge(
            df_updates.alias("s"),
            f"t.{business_key} = s.{business_key} AND t._is_current = true"
        ).whenMatchedUpdate(set = {
            "_is_current": lit(False),
            "_valid_to": current_timestamp(),
            "_last_modified_date": current_timestamp()
        }).execute()
        
    # Append new records
    if df_inserts_sk.count() > 0:
        df_inserts_sk.write.format("delta").mode("append").save(gold_path)

# COMMAND ----------

# 7. Optimize and Return
spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY ({business_key})")
records_processed = df_source.count()
dbutils.notebook.exit(str(records_processed))