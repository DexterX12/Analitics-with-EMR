create databe athena;

create external table athena.tabla(
country string, `Avg Life expectancy at birth, female (years)` float,
`Avg Life expectancy at birth, male (years)` float,
`Avg Life expectancy at birth, total (years)` float,
`Avg Mortality rate, adult, female (per 1,000 female adults)` float,
`Avg Mortality rate, adult, male (per 1,000 male adults)` float,
`Avg Number of infant deaths` float
) stored as parquet location 's3://datasetproject3/refined/'