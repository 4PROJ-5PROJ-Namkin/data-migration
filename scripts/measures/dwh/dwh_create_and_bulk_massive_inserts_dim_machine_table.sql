SELECT [machineId]
  INTO [DWH_PRODUCTION_1].[dbo].[dim_machine]		
  FROM [ODS_PRODUCTION].[dbo].[dim_machine]

ALTER TABLE [DWH_PRODUCTION_1].[dbo].[dim_machine]
ALTER COLUMN [machineId] INT NOT NULL;

ALTER TABLE [DWH_PRODUCTION_1].[dbo].[dim_machine]
ADD CONSTRAINT PK_DIM_MACHINE PRIMARY KEY (machineId)