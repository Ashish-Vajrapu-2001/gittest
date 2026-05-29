# Troubleshooting Guide

## 1. Initial Load Runs Every Time
**Symptom**: The pipeline extracts the full table every run instead of switching to CDC.
**Cause**: The `initial_load_completed` flag is not being updated to `1`.
**Resolution**:
- Check the Databricks notebook `Initial_Load_Dynamic`. Ensure the JDBC update step succeeds.
- Verify `sp_UpdateTableMetadata` is receiving `@MarkInitialLoadComplete = 1`.
- Manually update if necessary: `UPDATE control.table_metadata SET initial_load_completed = 1 WHERE table_name = 'YourTable';`

## 2. MERGE Operation Fails
**Symptom**: Databricks notebook fails during the `target_table.merge(...)` step.
**Cause**: 
- Primary key mismatch or duplicates in source data causing ambiguous matches.
- Delta table does not exist (Initial load didn't run).
**Resolution**:
- Ensure the pre-MERGE deduplication logic in `Incremental_CDC_Dynamic.py` is intact.
- Verify `primary_key_columns` in `control.table_metadata` exactly matches the source table.
- If Delta table is missing, reset `initial_load_completed = 0` and rerun.

## 3. Change Tracking Version Invalid
**Symptom**: `sp_GetCDCChanges` fails with "Change tracking version is invalid".
**Cause**: The `last_sync_version` stored in metadata is older than the retention period of the source database.
**Resolution**:
1. Reset the table for a full load: