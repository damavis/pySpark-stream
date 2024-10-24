from pyspark import SparkContext
from pyspark.streaming import StreamingContext, DStream

sc = SparkContext("local[*]", "EjemploStreamContext")
ssc = StreamingContext(sc, 1)

input:DStream[str] = ssc.socketTextStream("localhost", 9999)

def transform_text(text):
    return text.upper()

transformed_lines = input.map(transform_text)

transformed_lines.pprint()

ssc.start()
ssc.awaitTermination()