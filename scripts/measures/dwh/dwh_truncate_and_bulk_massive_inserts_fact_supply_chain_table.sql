TRUNCATE TABLE [DWH_PRODUCTION].[dbo].[fact_supply_chain];

INSERT INTO [DWH_PRODUCTION].[dbo].[fact_supply_chain]
SELECT DISTINCT
    MachineProduction.machineId,
	PartProduction.partId,
    main.materialId,
	MachineProduction.timeId,
	main.materialPrice,
    main.materialPriceDate,
    main.partDefaultPrice,
	MachineProduction.CountProductionMachine,
    PartProduction.CountProductionPart,
    GarbageCount.GarbageProduction
FROM 
    [ODS_PRODUCTION].[dbo].[fact_supply_chain] AS main
INNER JOIN 
    (SELECT 
         COUNT(timeOfProduction) AS CountProductionMachine, 
         timeId, 
         machineId
     FROM [ODS_PRODUCTION].[dbo].[fact_supply_chain]
     GROUP BY timeId, machineId) AS MachineProduction
ON main.timeId = MachineProduction.timeId AND main.machineId = MachineProduction.machineId
INNER JOIN 
    (SELECT 
         COUNT(timeOfProduction) AS CountProductionPart, 
         timeId,
         partId
     FROM [ODS_PRODUCTION].[dbo].[fact_supply_chain]
     GROUP BY timeId, partId) AS PartProduction
ON main.timeId = PartProduction.timeId AND main.partId = PartProduction.partId
INNER JOIN
    (SELECT 
         COUNT(timeOfProduction) AS GarbageProduction, 
         timeId
     FROM [ODS_PRODUCTION].[dbo].[fact_supply_chain]
     WHERE isDamaged = 1
     GROUP BY timeId) AS GarbageCount
ON main.timeId = GarbageCount.timeId
ORDER BY MachineProduction.machineId;