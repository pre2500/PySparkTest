from pyspark.sql import SparkSession
import os


def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
  spark,sc = init_spark()
  nums = sc.parallelize([1,2,3,4])
  print(nums.map(lambda x: x*x).collect())
  for param in os.environ.keys():
    print "%20s %s" % (param,os.environ[param])


if __name__ == '__main__':
  main()
