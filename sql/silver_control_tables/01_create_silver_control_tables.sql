-- Create control schema if it does not exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]');
END
GO

-- 1. control.silver_table_config
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('control.silver_table_config'))
BEGIN
    CREATE TABLE control.silver_table_config (
        silver_table_id INT IDENTITY(1,1) PRIMARY KEY,
        source_system VARCHAR(50) NOT NULL,
        schema_name VARCHAR(100) NOT NULL,
        source_bronze_table VARCHAR(255) NOT NULL,
        target_silver_table VARCHAR(255) NOT NULL,
        transformation_type VARCHAR(50) NOT NULL CHECK (transformation_type IN ('CLEANSE', 'TRANSFORM', 'AGGREGATE')),
        scd_type INT NOT NULL DEFAULT 1,
        is_active BIT NOT NULL DEFAULT 1,
        last_processed_version BIGINT NULL,
        last_load_status VARCHAR(50) NULL,
        last_load_timestamp DATETIME2 NULL,
        last_pipeline_run_id VARCHAR(100) NULL,
        created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        modified_date DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

-- 2. control.silver_transformation_rules
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('control.silver_transformation_rules'))
BEGIN
    CREATE TABLE control.silver_transformation_rules (
        rule_id INT IDENTITY(1,1) PRIMARY KEY,
        silver_table_id INT NOT NULL,
        rule_name VARCHAR(100) NOT NULL,
        rule_type VARCHAR(50) NOT NULL CHECK (rule_type IN ('FILTER', 'TRANSFORM', 'RENAME', 'CAST', 'DEDUPE')),
        source_column VARCHAR(255) NULL,
        target_column VARCHAR(255) NULL,
        transformation_expression VARCHAR(MAX) NULL,
        execution_sequence INT NOT NULL DEFAULT 10,
        is_active BIT NOT NULL DEFAULT 1,
        created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT FK_silver_transformation_rules_table_config FOREIGN KEY (silver_table_id) 
        REFERENCES control.silver_table_config(silver_table_id)
    );
END
GO

-- 3. control.silver_execution_log
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('control.silver_execution_log'))
BEGIN
    CREATE TABLE control.silver_execution_log (
        log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_run_id VARCHAR(100) NOT NULL,
        silver_table_id INT NOT NULL,
        records_read BIGINT NOT NULL DEFAULT 0,
        records_written BIGINT NOT NULL DEFAULT 0,
        records_filtered BIGINT NOT NULL DEFAULT 0,
        start_time DATETIME2 NOT NULL,
        end_time DATETIME2 NOT NULL,
        execution_status VARCHAR(50) NOT NULL,
        error_message NVARCHAR(MAX) NULL,
        CONSTRAINT FK_silver_execution_log_table_config FOREIGN KEY (silver_table_id) 
        REFERENCES control.silver_table_config(silver_table_id)
    );
END
GO