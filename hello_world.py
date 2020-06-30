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
  print(os.getenv('SPARK_SUBMIT_OPTS'))
  download_file_from_s3()
  
def download_file_from_s3():
   s3 = boto3.resource('s3')
   try:
       s3.Bucket('tardis-im-clients').download_file(KEY, 'keycloak_client_list.csv')
    except botocore.exceptions.ClientError as e:
       if e.response['Error']['Code'] == "404":
           print("The object does not exist.")
       else:
           raise


if __name__ == '__main__':
  main()
