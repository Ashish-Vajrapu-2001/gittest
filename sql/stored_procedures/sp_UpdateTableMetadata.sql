CREATE OR ALTER PROCEDURE [control].[sp_UpdateTableMetadata]
    @TableId INT,
    @Status VARCHAR(50),
    @PipelineRunId VARCHAR(100),
    @RecordsLoaded BIGINT,
    @SyncVersion BIGINT,
    @MarkInitialLoadComplete BIT,
    @ErrorMessage VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- 1. Update Table Metadata
        UPDATE [control].[table_metadata]
        SET 
            last_load_status = @Status,
            last_load_timestamp = GETDATE(),
            last_pipeline_run_id = @PipelineRunId,
            records_loaded = @RecordsLoaded,
            modified_date = GETDATE(),
            -- Update sync version only on success
            last_sync_version = CASE WHEN @Status = 'SUCCESS' THEN @SyncVersion ELSE last_sync_version END,
            -- CRITICAL: Update initial_load_completed flag
            initial_load_completed = CASE 
                                        WHEN @Status = 'SUCCESS' AND @MarkInitialLoadComplete = 1 THEN 1 
                                        ELSE initial_load_completed 
                                     END
        WHERE table_id = @TableId;

        -- 2. Insert into Pipeline Execution Log
        INSERT INTO [control].[pipeline_execution_log] 
        (pipeline_run_id, table_id, execution_status, records_processed, start_time, end_time, error_message)
        VALUES 
        (@PipelineRunId, @TableId, @Status, @RecordsLoaded, GETDATE(), GETDATE(), @ErrorMessage);

        COMMIT TRANSACTION;

        -- Return updated record for verification
        SELECT * FROM [control].[table_metadata] WHERE table_id = @TableId;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();

        RAISERROR (@ErrorMsg, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO