import logging
import os
import sys

from pyspark.sql.functions import to_date, when

logging. \
    basicConfig(filename="logFilePOCTwoOG.txt", filemode='w',
                format='%(asctime)s %(levelname)s-%(message)s')

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

logging.warning("Starting the utilities class")


class UtilityNew:
    def __init__(self):
        self.spark = SparkSession.builder.appName('My POC Two App').getOrCreate()

    def csvRead(self, file):
        try:
            logging.warning("File is reading from Utility class")
            return self.spark.read.csv(file, inferSchema=True, header=True)

        except Exception as e:
            print(e)
            logging.error("File not found")

    def parquetWrite(self, data, name):
        try:
            data.write.mode("overwrite").option("header", "true"). \
                partitionBy("Year", "Month").format("parquet").save(name)
            logging.warning("File is saving from Utility class")

        except Exception as e:
            print(e)
            logging.error("File save error")

    def stringToDate(self, data):
        logging.warning("Changed the DATE column format from sting to date")
        return data.withColumn('DATE', to_date(data.DATE, 'yyyy-MM-dd'))

"""
myUtility = UtilityNew()
mydf = myUtility.csvRead("updatedbank")
mydf = myUtility.stringToDate(mydf)
mydf.show()
mydf.printSchema()
"""
