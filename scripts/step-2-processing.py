from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("S3 to Spark") \
    .getOrCreate()

# Importa e infiere el esquema de los datos
df = spark.read.option("inferSchema", "true").parquet("s3://datasetproject3/trusted/")
#df = spark.read.option("inferSchema", "true").option("header", "true").csv("/content/gdrive/MyDrive/out/clean/part-00000-12828d71-c17a-4067-a0a6-fec6d1bdf6a4-c000.csv")

filtered = df.select("Indicator Name") # Obten los indicadores de vida

# Crea el esquema de los datos para los promedios a partir de cada indicador UNICO
schema = StructType(
    [StructField("Country", StringType(), True)] +
    [
      StructField("Avg " + i, FloatType(), True)
      for i in filtered.distinct().orderBy("Indicator Name").rdd.flatMap(lambda x: x).collect()
    ]
)

countries = df.select("Country Name").distinct().collect() # Obten los paises
base_df = spark.createDataFrame([], schema) # Crea el DataFrame con el esquema anteriormente creado

# Las columnas de los años de los datos, dado por el minimo y maximo presentes en el dataset
years = [str(year) for year in range(int(df.columns[2]), int(df.columns[-1]))]

for country in countries:
    country_name = country[0]

    # Obten la data de todos los indicadores de 1 solo pais
    country_data = df.filter(df["Country Name"] == country_name).orderBy("Indicator Name")

    # Promedia cada indicador
    country_avg = country_data.withColumn("avg",
                                        sum([F.col(c) for c in years]) / len(years)) 
    
    # Asegurarse de que los valores nulos sean 0 (No suman nada, puesto que se hace un promedio unicamente)
    country_avg = country_avg.withColumn("avg", F.coalesce("avg", F.lit(0)))
    
    # Obten el promedio de cada indicador como una lista unidimensional
    averages = country_avg.select("avg").orderBy("Indicator Name").rdd.flatMap(lambda x: x).collect()

    # Colocar el nombre del pais como valor inicial, para indicar similitud con el esquema creado
    country_row = [[country_name] + averages]

    # Crear una fila de dicho pais con los promedios
    df_row = spark.createDataFrame(country_row, schema)

    # Unir fila con el DataFrame base de todos los paises
    base_df = base_df.union(df_row)

path_out='s3://datasetproject3/refined/'
base_df.write.mode("overwrite").parquet(path_out)