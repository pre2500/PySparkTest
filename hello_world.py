from pyspark.sql import SparkSession
import os
import time



def init_spark():
    spark = SparkSession.builder.appName('HelloWorld').getOrCreate()
    sc = spark.sparkContext
    return (spark, sc)


def main():
    (spark, sc) = init_spark()
    time.sleep(60)
    print os.getenv('SPARK_SUBMIT_OPTS')
    for item, value in os.environ.items():
        print('{}: {}'.format(item, value))


if __name__ == '__main__':
    main()


