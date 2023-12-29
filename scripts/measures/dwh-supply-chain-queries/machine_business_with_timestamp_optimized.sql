-- TIMESTAMP VERSION OF timeOfProduction column
-- The period is based on the date difference between a fixed start period and end period for this use case
-- This script is a full-fledged optimized of the previous script which make a usage of two CTE (Common Table Expression) cross-joined

WITH TotalUtilizationCTE AS (
    SELECT
        COUNT(*) AS TotalUtilization
    FROM fact_table
    WHERE timeOfProduction >= DATEDIFF(SECOND, '19700101', '2010-01-01') AND timeOfProduction < DATEDIFF(SECOND, '19700101', '2020-01-01')
),

MachineProductionCTE AS (
    SELECT
        machineId,
        COUNT(*) AS TotalProduction
    FROM fact_table
    WHERE timeOfProduction >= DATEDIFF(SECOND, '19700101', '2010-01-01') AND timeOfProduction < DATEDIFF(SECOND, '19700101', '2014-01-01')
    GROUP BY machineId
)

SELECT TOP 100
    M.machineId,
    M.TotalProduction,
    T.TotalUtilization,
    CAST((M.TotalProduction * 100.0) / T.TotalUtilization AS FLOAT) AS UtilizationPercentage
FROM MachineProductionCTE AS M
CROSS JOIN TotalUtilizationCTE AS T
ORDER BY M.TotalProduction ASC;
