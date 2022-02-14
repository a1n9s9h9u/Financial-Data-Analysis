import logging
import os
import sys

logging.basicConfig(filename="logFilePOCTwo.txt", filemode='a', format='%(asctime)s %(levelname)s-%(message)s')

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('My POC Two Class App').getOrCreate()

# csv to dataframe
def readCsv(csvfile):
    dataframe = spark.read.csv(csvfile, inferSchema=True, header=True)
    return dataframe

df = readCsv("bankcsv.csv")
df.show(10)
df.printSchema()

logging.warning("This is the end of the test file")
