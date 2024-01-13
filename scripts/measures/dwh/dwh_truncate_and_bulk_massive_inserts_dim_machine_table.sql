TRUNCATE TABLE [DWH_PRODUCTION].[dbo].[dim_machine];

INSERT INTO [DWH_PRODUCTION].[dbo].[dim_machine]
SELECT [machineId]
  FROM [ODS_PRODUCTION].[dbo].[dim_machine]