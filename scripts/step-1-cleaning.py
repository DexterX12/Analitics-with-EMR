from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("S3 to Spark") \
    .getOrCreate()

# Importa e infiere el esquema de los datos
df = spark.read.option("inferSchema", "true").option("header", "true").csv("s3://datasetproject3/raw/OpenBank/world_development_indicator/WDICSV.csv")


# Selecciona los años 2013-2023
pertinent_columns = df.select(df.columns[:3:2] + df.columns[-12:-1])

study_indicators = [
    "Life expectancy at birth, female (years)",
    "Life expectancy at birth, male (years)",
    "Life expectancy at birth, total (years)",
    "Mortality rate, adult, female (per 1,000 female adults)",
    "Mortality rate, adult, male (per 1,000 male adults)",
    "Number of infant deaths"
]

# Dado los indicadores anteriores, los filtra entre todos los datos del dataset
filtered_by_indicators = pertinent_columns.filter(col("Indicator Name").isin(study_indicators))

path_out='s3://datasetproject3/trusted/'
filtered_by_indicators.write.mode("overwrite").parquet(path_out)
#filtered_by_indicators.coalesce(1).write.format("csv").option("header","true").save(pathcsv_out, mode='overwrite')