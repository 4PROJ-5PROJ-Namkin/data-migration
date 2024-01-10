SELECT [partId]
      ,[timeToProduce]
  INTO [DWH_PRODUCTION].[dbo].[dim_part_information]
  FROM [ODS_PRODUCTION].[dbo].[dim_part_information]

ALTER TABLE [DWH_PRODUCTION].[dbo].[dim_part_information]
ALTER COLUMN [partId] INT NOT NULL;

ALTER TABLE [DWH_PRODUCTION].[dbo].[dim_part_information]
ADD CONSTRAINT PK_DIM_PART_INFORMATION PRIMARY KEY ([partId])
