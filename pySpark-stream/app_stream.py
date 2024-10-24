from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, count
from pyspark.sql.types import StructType, IntegerType, StringType

abs = Path(__file__).resolve()
cities_path = abs.parent.parent / 'var' / 'cities'

spark = SparkSession.builder.appName("EjemploPandas").getOrCreate()

cities_schema = StructType() \
    .add("LatD", IntegerType()) \
    .add("LatM", IntegerType()) \
    .add("LatS", IntegerType()) \
    .add("NS", StringType()) \
    .add("LonD", IntegerType()) \
    .add("LonM", IntegerType()) \
    .add("LonS", IntegerType()) \
    .add("EW", StringType()) \
    .add("City", StringType()) \
    .add("State", StringType())

df:DataFrame = spark.readStream.option("sep", ",").option("header", "true").schema(cities_schema).csv(
    str(cities_path))

def filter_func(iterator):
    for pdf in iterator:
        yield pdf[pdf.State == 'GA']

filtered_df = df.mapInPandas(filter_func,df.schema,barrier=True)


result = filtered_df.groupBy("State").agg(avg("LatD").alias("latm_avg"), avg("LatM").alias("latm_avg"),
                                 avg("LatS").alias("lats_avg"), count("State").alias("count"))

query = result.writeStream.outputMode("complete").format("console").start(truncate=False)

query.awaitTermination()

