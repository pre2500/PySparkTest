from pyspark.sql import SparkSession
import os
import time



def init_spark():
    spark = SparkSession.builder.appName('HelloWorld').getOrCreate()
    sc = spark.sparkContext
    return (spark, sc)


def main():
    for i in range(3):
        (spark, sc) = init_spark()
        print os.getenv('SPARK_SUBMIT_OPTS')
        time.sleep(60)


if __name__ == '__main__':
    main()


