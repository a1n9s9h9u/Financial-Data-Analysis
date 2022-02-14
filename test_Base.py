import os
import sys

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('My POC Two Test App').getOrCreate()

data = [("409000611074", 100, 50, "2015-05-02"), ("409000493201", 200, 100, "2016-07-09"),
        ("409000405747", 300, 70, "2015-01-04"), ("409000438611", 400, 90, "2016-11-08"),
        ("1196711", 200, 10, "2014-06-11"), ("1196428", 100, 500, "2016-04-01")]

schema = StructType([
        StructField("Account_No", StringType(), True),
        StructField("DEPOSIT_AMT", IntegerType(), True),
        StructField("WITHDRAWAL_AMT", IntegerType(), True),
        StructField("DATE", StringType(), True)
        ])

dummydf = spark.createDataFrame(data=data, schema=schema)
# dummydf.show()
# dummydf.printSchema()

# dummydf.write.mode("overwrite").options(header=True).csv("dummy.csv")

retaildummydata = [("1", 5, 2015, 100), ("2", 7, 2016, 200),
        ("3", 1, 2015, 300), ("4", 11, 2016, 400),
        ("5", 6, 2014, 200), ("6", 4, 2016, 100)]

retaildummyschema = StructType([
        StructField("Account_No", StringType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Total_Deposits", IntegerType(), True)
        ])

dummyretaildf = spark.createDataFrame(data=retaildummydata, schema=retaildummyschema)
# dummyretaildf.show()
