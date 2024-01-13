TRUNCATE TABLE [DWH_PRODUCTION].[dbo].[dim_part_information];

INSERT INTO [DWH_PRODUCTION].[dbo].[dim_part_information]
SELECT [partId]
      ,[timeToProduce]
  INTO [DWH_PRODUCTION].[dbo].[dim_part_information]
  FROM [ODS_PRODUCTION].[dbo].[dim_part_information]