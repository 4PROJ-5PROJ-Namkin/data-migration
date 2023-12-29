-- The period is based on the date difference between a fixed start period and end period for this use case

SELECT TOP 100
    partId,
    COUNT(*) AS PartProduced
FROM fact_table
WHERE timeOfProduction >= DATEDIFF(SECOND, '19700101', '2010-01-01') 
    AND timeOfProduction < DATEDIFF(SECOND, '19700101', '2014-01-01')
GROUP BY partId
ORDER BY PartProduced ASC;
