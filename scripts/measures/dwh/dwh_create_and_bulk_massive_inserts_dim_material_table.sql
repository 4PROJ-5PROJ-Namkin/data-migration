SELECT [materialId]
      ,[name]
  INTO [DWH_PRODUCTION_1].[dbo].[dim_material]
  FROM [ODS_PRODUCTION].[dbo].[dim_material]

ALTER TABLE [DWH_PRODUCTION_1].[dbo].[dim_material]
ALTER COLUMN [materialId] INT NOT NULL;

ALTER TABLE [DWH_PRODUCTION_1].[dbo].[dim_material]
ADD CONSTRAINT PK_DIM_MATERIAL PRIMARY KEY ([materialId])
