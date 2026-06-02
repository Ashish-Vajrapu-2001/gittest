# Troubleshooting Guide

## 1. Initial Load Runs Every Time
**Symptom:** The pipeline keeps executing `PL_Initial_Load_Single_Table` instead of incremental.
**Cause:** The `initial_load_completed` flag is not being set to 1.
**Resolution:**
- Check the Databricks notebook `Initial_Load_Dynamic`. Ensure the JDBC update to `sp_UpdateTableMetadata` is succeeding.
- Verify `sp_UpdateTableMetadata` has the logic: `WHEN @Status = 'SUCCESS' AND @MarkInitialLoadComplete = 1 THEN 1`.

## 2. MERGE Fails with "Ambiguous Match"
**Symptom:** Databricks incremental notebook fails during the `MERGE INTO` operation.
**Cause:** Multiple updates for the same primary key exist in the CDC batch.
**Resolution:**
- The `Incremental_CDC_Dynamic.py` notebook includes a deduplication step using `Window.partitionBy(*pk_cols).orderBy(col("SYS_CHANGE_VERSION").desc())`. Ensure the `primary_key_columns` in `control.table_metadata` exactly match the source table.

## 3. DELTA_INVALID_FORMAT Error
**Symptom:** Databricks fails to read the Delta table.
**Cause:** ADF wrote Parquet files directly into the Delta table path instead of the staging path.
**Resolution:**
- Ensure `DS_Parquet_Bronze_Staging` points to `bronze/staging/...`.
- Never point ADF Copy Activity directly to `bronze/SRC-001/...`.

## 4. Change Tracking Version Invalid
**Symptom:** `sp_GetCDCChanges` fails because the `last_sync_version` is too old.
**Cause:** The source database cleaned up old change tracking data.
**Resolution:**
- Reset the table for a full load: