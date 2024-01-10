SELECT [timeId]
      ,[date]
      ,[year]
      ,[month]
      ,[day]
      ,[semester]
      ,[quarter]
  INTO [DWH_PRODUCTION_1].[dbo].[dim_time]
  FROM [ODS_PRODUCTION].[dbo].[dim_time]

ALTER TABLE [DWH_PRODUCTION_1].[dbo].[dim_time]
ALTER COLUMN [timeId] INT NOT NULL;

ALTER TABLE [DWH_PRODUCTION_1].[dbo].[dim_time]
ADD CONSTRAINT PK_DIM_TIME PRIMARY KEY ([timeId])

