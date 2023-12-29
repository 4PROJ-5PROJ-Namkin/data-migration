-- TIMESTAMP VERSION OF timeOfProduction column
-- The period is based on the date difference between a fixed start period and end period for this use case

SELECT TOP 100
    machineId,
    COUNT(*) AS TotalProduction,
    (SELECT COUNT(*) FROM fact_table WHERE timeOfProduction >= DATEDIFF(SECOND, '19700101', '2010-01-01') AND timeOfProduction < DATEDIFF(SECOND, '19700101', '2020-01-01')) AS TotalUtilization,
    CAST(
        (COUNT(*) * 100.0) / 
        (SELECT COUNT(*) FROM fact_table WHERE timeOfProduction >= DATEDIFF(SECOND, '19700101', '2010-01-01') AND timeOfProduction < DATEDIFF(SECOND, '19700101', '2020-01-01'))
        AS FLOAT
    ) AS UtilizationPercentage
FROM fact_table
WHERE timeOfProduction >= DATEDIFF(SECOND, '19700101', '2010-01-01') AND timeOfProduction < DATEDIFF(SECOND, '19700101', '2014-01-01')
GROUP BY machineId
ORDER BY TotalProduction ASC
