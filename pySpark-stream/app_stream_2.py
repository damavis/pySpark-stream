from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

abs = Path(__file__).resolve()
reader_path = abs.parent.parent / 'var' / 'lorem'

spark = SparkSession.builder.appName("EjemploStructuredStream").getOrCreate()

df:DataFrame = spark.readStream.format("text").load(str(reader_path))

word_counts = df \
    .select(explode(split(df.value, " ")).alias("word")) \
    .groupBy("word").count()

query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
