CREATE OR ALTER PROCEDURE [control].[sp_GetCDCChanges]
    @SchemaName VARCHAR(100),
    @TableName VARCHAR(100),
    @PrimaryKeyColumns VARCHAR(255),
    @LastSyncVersion BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @JoinConditions NVARCHAR(MAX) = '';
    DECLARE @PK_Cols_CT NVARCHAR(MAX) = '';
    
    -- Parse comma-separated Primary Keys for dynamic SQL
    DECLARE @PK_Table TABLE (PK_Col VARCHAR(100));
    INSERT INTO @PK_Table (PK_Col)
    SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Build Join Conditions: T.PK1 = CT.PK1 AND T.PK2 = CT.PK2
    SELECT @JoinConditions = @JoinConditions + 'T.[' + PK_Col + '] = CT.[' + PK_Col + '] AND '
    FROM @PK_Table;
    
    -- Remove trailing ' AND '
    SET @JoinConditions = LEFT(@JoinConditions, LEN(@JoinConditions) - 4);

    -- Build CT PK selection: CT.PK1, CT.PK2
    SELECT @PK_Cols_CT = @PK_Cols_CT + 'CT.[' + PK_Col + '], '
    FROM @PK_Table;
    
    -- Remove trailing comma and space
    SET @PK_Cols_CT = LEFT(@PK_Cols_CT, LEN(@PK_Cols_CT) - 1);

    -- Construct Dynamic SQL using CHANGETABLE
    SET @SQL = '
        SELECT 
            CT.SYS_CHANGE_OPERATION,
            CT.SYS_CHANGE_VERSION,
            ' + @PK_Cols_CT + ',
            CHANGE_TRACKING_CURRENT_VERSION() AS _current_sync_version,
            T.*
        FROM CHANGETABLE(CHANGES [' + @SchemaName + '].[' + @TableName + '], ' + CAST(@LastSyncVersion AS VARCHAR) + ') AS CT
        LEFT JOIN [' + @SchemaName + '].[' + @TableName + '] AS T 
            ON ' + @JoinConditions + ';
    ';

    -- Execute Dynamic SQL
    EXEC sp_executesql @SQL;
END;
GO