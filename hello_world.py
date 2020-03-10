import sys
from random
import random
from operator
import add

from pyspark.sql
import SparkSession

spark = SparkSession\
  .builder\
  .appName("HelloWorld")\
  .getOrCreate()

data = spark.parallelize(list("Hello World"))
counts = data.map(lambda x:
  (x, 1)).reduceByKey(add).sortBy(lambda x: x[1],
  ascending = False).collect()

for (word, count) in counts:
  print("{}: {}".format(word, count))

