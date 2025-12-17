from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("RDDWordCountFromFile").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile(sys.argv[1])

word_count = (
    rdd.flatMap(lambda line: line.split(" "))
       .map(lambda word: (word, 1))
       .reduceByKey(lambda a, b: a + b)
       .sortBy(lambda x: x[1], ascending=False)
)

for w in word_count.collect():
    print(w)
print("end------------------------------------------")

