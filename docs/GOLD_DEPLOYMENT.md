# Gold Layer Deployment Guide

## Prerequisites
1. Bronze and Silver layers must be fully deployed and populated.
2. Azure SQL Database (Control DB) must be accessible.
3. Databricks workspace must have Unity Catalog enabled.
4. `dim_date` must be pre-populated in the Gold layer (2015-2030).

## Deployment Steps

### 1. Control Database Setup
Execute the SQL scripts in the following order against the Control Database:
1. `sql/gold_control_tables/01_create_gold_control_tables.sql`
2. `sql/gold_control_tables/02_populate_gold_config.sql`
3. `sql/gold_stored_procedures/sp_UpdateGoldTableStatus.sql`
4. `sql/gold_stored_procedures/sp_GetGoldAggregationRules.sql`
5. `sql/gold_stored_procedures/sp_GetGoldDimensionConfig.sql`

### 2. Databricks Setup
1. Import the following notebooks into `/Shared/gold/`:
   - `Gold_Build_Dimension.py`
   - `Gold_Build_Fact.py`
   - `Gold_Build_Aggregate.py`
   - `Gold_Data_Mart.py`
2. Run `Gold_Data_Mart.py` once to establish the Unity Catalog views.

### 3. Azure Data Factory Setup
1. Deploy Linked Services (ensure placeholders are replaced with Key Vault references).
2. Deploy Datasets (`DS_Delta_Silver`, `DS_Delta_Gold`, `DS_ControlDB_Gold`).
3. Deploy Pipelines in this order:
   - `PL_Gold_Build_Dimension`
   - `PL_Gold_Build_Fact`
   - `PL_Gold_Build_Aggregate`
   - `PL_Gold_Orchestrator`
   - `PL_Medallion_Master`

### 4. Initial Execution
1. Trigger `PL_Medallion_Master` with `run_bronze=false`, `run_silver=false`, `run_gold=true`.
2. Verify that `dim_customer` contains the `-1` Unknown Member.
3. Verify that `agg_customer_clv` is populated successfully.