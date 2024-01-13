TRUNCATE TABLE [DWH_PRODUCTION].[dbo].[dim_contract];

INSERT INTO [DWH_PRODUCTION].[dbo].[dim_contract]
SELECT [contractId]
      ,[clientName]
  FROM [ODS_PRODUCTION].[dbo].[dim_contract]