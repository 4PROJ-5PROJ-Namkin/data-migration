TRUNCATE TABLE [DWH_PRODUCTION].[dbo].[dim_material];

INSERT INTO [DWH_PRODUCTION].[dbo].[dim_material]
SELECT [materialId]
      ,[name]
  INTO [DWH_PRODUCTION].[dbo].[dim_material]
  FROM [ODS_PRODUCTION].[dbo].[dim_material]