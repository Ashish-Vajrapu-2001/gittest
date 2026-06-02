# Silver Layer Transformations Guide

The Silver layer utilizes a metadata-driven transformation engine. Transformations are defined in the `control.silver_transformation_rules` table and applied dynamically by the Databricks notebook `Silver_Transform_Dynamic.py`.

## Available Transformation Types

| Rule Type | Description | Example Expression |
|-----------|-------------|--------------------|
| **CAST** | Changes the data type of a column. | `bigint`, `decimal(15,2)`, `date`, `timestamp` |
| **TRANSFORM** | Applies a SQL function to modify data. | `lower(trim(EMAIL))`, `coalesce(DISCOUNT, 0)` |
| **RENAME** | Renames a column without modifying data. | N/A (Uses `source_column` and `target_column`) |
| **FILTER** | Removes rows that do not match the condition. | `order_status NOT IN ('Cancelled', 'Returned')` |
| **DEDUPE** | Removes duplicate rows based on key columns. | `customer_id`, `order_id, product_id` |

## How to Add a New Transformation Rule

To add a new rule for an existing table, insert a record into `control.silver_transformation_rules`.

**Example: Adding a rule to uppercase a new column `country_code` in `silver.addresses`:**