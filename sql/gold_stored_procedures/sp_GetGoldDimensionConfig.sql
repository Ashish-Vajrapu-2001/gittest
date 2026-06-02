CREATE PROCEDURE [control].[sp_GetGoldDimensionConfig]
    @GoldTableId INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        c.target_gold_table,
        d.scd_type,
        d.business_key_columns,
        d.tracked_columns
    FROM control.gold_dimension_config d
    INNER JOIN control.gold_table_config c ON d.gold_table_id = c.gold_table_id
    WHERE d.gold_table_id = @GoldTableId;
END
GO