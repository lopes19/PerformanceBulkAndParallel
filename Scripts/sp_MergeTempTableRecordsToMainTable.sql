
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF EXISTS (
     SELECT  *
     FROM    dbo.sysobjects
     WHERE   id = OBJECT_ID(N'[dbo].[sp_MergeTempTableRecordsToMainTable]'))
BEGIN
	DROP PROCEDURE [dbo].[sp_MergeTempTableRecordsToMainTable]
END

GO

CREATE PROCEDURE [dbo].[sp_MergeTempTableRecordsToMainTable]
	
AS
BEGIN

	SET NOCOUNT ON;
	
	BEGIN TRY
		BEGIN TRANSACTION

			MERGE [MainTable] AS target
			USING (

				SELECT [TempKey]
					  ,[Id]
					  ,[Description]
				  FROM [TempTable]
    
			)
			AS source ([TempKey],[Id],[Description])

			ON (target.[Id] = source.[Id])
	
			WHEN MATCHED THEN 
				UPDATE SET [Description] = source.[Description]

			WHEN NOT MATCHED THEN
				INSERT ([Id],[Description])
				VALUES (source.[Id]
						,source.[Description]
				);


			--Clear temporary table
			DELETE [TempTable]

			SELECT
				0 AS Code,
				'Merge finished successfully!' AS [Message];

		COMMIT TRANSACTION
	END TRY
	BEGIN CATCH
		ROLLBACK TRANSACTION
		SELECT 
			ERROR_NUMBER() AS Code,
			ERROR_MESSAGE() AS [Message];
	END CATCH

END