SELECT [contractId]
      ,[clientName]
  INTO [DWH_PRODUCTION].[dbo].[dim_contract]
  FROM [ODS_PRODUCTION].[dbo].[dim_contract]

ALTER TABLE [DWH_PRODUCTION].[dbo].[dim_contract]
ALTER COLUMN [contractId] INT NOT NULL;

ALTER TABLE [DWH_PRODUCTION].[dbo].[dim_contract]
ADD CONSTRAINT PK_DIM_CONTRACT PRIMARY KEY ([contractId])
