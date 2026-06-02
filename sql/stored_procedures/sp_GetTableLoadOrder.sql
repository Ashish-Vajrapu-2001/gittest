CREATE OR ALTER PROCEDURE [control].[sp_GetTableLoadOrder]
    @SourceSystemId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    WITH DependencyGraph AS (
        -- Base case: Tables with no dependencies
        SELECT 
            t.table_id,
            t.schema_name,
            t.table_name,
            t.load_priority,
            0 AS dependency_level
        FROM [control].[table_metadata] t
        LEFT JOIN [control].[load_dependencies] d ON t.table_id = d.table_id
        WHERE d.dependency_id IS NULL
          AND t.is_active = 1
          AND (@SourceSystemId IS NULL OR t.source_system_id = @SourceSystemId)

        UNION ALL

        -- Recursive case: Tables that depend on others
        SELECT 
            t.table_id,
            t.schema_name,
            t.table_name,
            t.load_priority,
            dg.dependency_level + 1 AS dependency_level
        FROM [control].[table_metadata] t
        INNER JOIN [control].[load_dependencies] d ON t.table_id = d.table_id
        INNER JOIN DependencyGraph dg ON d.depends_on_table_id = dg.table_id
        WHERE t.is_active = 1
          AND (@SourceSystemId IS NULL OR t.source_system_id = @SourceSystemId)
    )
    SELECT 
        table_id,
        schema_name,
        table_name,
        MAX(dependency_level) AS max_dependency_level,
        load_priority
    FROM DependencyGraph
    GROUP BY table_id, schema_name, table_name, load_priority
    ORDER BY max_dependency_level ASC, load_priority ASC, table_name ASC;
END;
GO