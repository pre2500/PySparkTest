from pyspark.sql import SparkSession
import os
import time


def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
  spark,sc = init_spark()
  nums = sc.parallelize([1,2,3,4])
  time.sleep(300)
  print(nums.map(lambda x: x*x).collect())
  for param in os.environ.keys():
    print "%20s %s" % (param,os.environ[param])
    print("Now printing marathon app id and task id")
    print(os.getenv('MARATHON_APP_ID'))
    print(os.getenv('MESOS_TASK_ID'))
    


if __name__ == '__main__':
  main()
