-- Create schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]');
END
GO

-- 1. control.gold_table_config
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'gold_table_config')
BEGIN
    CREATE TABLE control.gold_table_config (
        gold_table_id INT IDENTITY(1,1) PRIMARY KEY,
        source_silver_tables VARCHAR(500) NOT NULL,
        target_gold_table VARCHAR(255) NOT NULL,
        table_type VARCHAR(50) NOT NULL CHECK (table_type IN ('FACT', 'DIMENSION', 'AGGREGATE', 'KPI')),
        refresh_frequency VARCHAR(50) NOT NULL DEFAULT 'DAILY',
        is_active BIT NOT NULL DEFAULT 1,
        last_refresh_timestamp DATETIME2 NULL,
        last_refresh_status VARCHAR(50) NULL,
        created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        modified_date DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

-- 2. control.gold_aggregation_rules
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'gold_aggregation_rules')
BEGIN
    CREATE TABLE control.gold_aggregation_rules (
        rule_id INT IDENTITY(1,1) PRIMARY KEY,
        gold_table_id INT NOT NULL FOREIGN KEY REFERENCES control.gold_table_config(gold_table_id),
        aggregation_name VARCHAR(255) NOT NULL,
        aggregation_type VARCHAR(50) NOT NULL CHECK (aggregation_type IN ('SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'DISTINCT_COUNT', 'CUSTOM_SQL')),
        source_column VARCHAR(255) NULL,
        target_column VARCHAR(255) NOT NULL,
        group_by_columns VARCHAR(1000) NULL,
        filter_expression VARCHAR(1000) NULL,
        is_active BIT NOT NULL DEFAULT 1
    );
END
GO

-- 3. control.gold_dimension_config
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'gold_dimension_config')
BEGIN
    CREATE TABLE control.gold_dimension_config (
        dimension_id INT IDENTITY(1,1) PRIMARY KEY,
        gold_table_id INT NOT NULL FOREIGN KEY REFERENCES control.gold_table_config(gold_table_id),
        scd_type INT NOT NULL CHECK (scd_type IN (1, 2, 3)),
        business_key_columns VARCHAR(500) NOT NULL,
        tracked_columns VARCHAR(MAX) NULL
    );
END
GO

-- 4. control.gold_execution_log
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'gold_execution_log')
BEGIN
    CREATE TABLE control.gold_execution_log (
        log_id INT IDENTITY(1,1) PRIMARY KEY,
        pipeline_run_id VARCHAR(100) NOT NULL,
        gold_table_id INT NOT NULL FOREIGN KEY REFERENCES control.gold_table_config(gold_table_id),
        records_processed BIGINT NOT NULL DEFAULT 0,
        execution_status VARCHAR(50) NOT NULL,
        start_time DATETIME2 NOT NULL,
        end_time DATETIME2 NOT NULL,
        error_message NVARCHAR(MAX) NULL
    );
END
GO