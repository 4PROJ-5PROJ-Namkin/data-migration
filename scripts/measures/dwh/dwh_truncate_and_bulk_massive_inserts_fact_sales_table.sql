TRUNCATE TABLE [DWH_PRODUCTION].[dbo].[fact_sales];

INSERT INTO [DWH_PRODUCTION].[dbo].[fact_sales]
SELECT main.[partId],
       main.[contractId],
       PartSales.[timeId],
       main.[cash],
       main.[date],
       PartSales.PartPurchasedPerDay,
       main.[cash] - (CostMaterial.CostMaterialPricePerDay + CostPart.CostPartPricePerDay) AS MarginPerDay
INTO [DWH_PRODUCTION].[dbo].[fact_sales]
FROM [ODS_PRODUCTION].[dbo].[fact_sales] AS main
INNER JOIN (
    SELECT CAST(REPLACE([date], '-', '') AS INT) AS timeId,
           COUNT(partId) AS PartPurchasedPerDay
    FROM [ODS_PRODUCTION].[dbo].[fact_sales]
    GROUP BY CAST(REPLACE([date], '-', '') AS INT)
) AS PartSales ON CAST(REPLACE(main.[date], '-', '') AS INT) = PartSales.[timeId]
INNER JOIN (
    SELECT materialDate,
           SUM(materialPrice) AS CostMaterialPricePerDay
    FROM (
        SELECT DISTINCT
               materialId,
               materialDate,
               materialPrice
        FROM [ODS_PRODUCTION].[dbo].[fact_supply_chain]
    ) AS DistinctMaterialPrices
    GROUP BY materialDate
) AS CostMaterial ON YEAR(main.[date]) = YEAR(CostMaterial.[materialDate])
INNER JOIN (
  SELECT 
    timeId,
    SUM(partDefaultPrice) AS CostPartPricePerDay
  FROM (
    SELECT 
      timeId,
      partDefaultPrice
    FROM 
      [ODS_PRODUCTION].[dbo].[fact_supply_chain]
  ) AS DistinctPartPrices
  GROUP BY 
    timeId
) AS CostPart 
  ON PartSales.[timeId] = CostPart.[timeId]
ORDER BY 
  main.[partId];