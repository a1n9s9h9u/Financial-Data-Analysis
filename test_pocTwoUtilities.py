import unittest
from pocTwoUtilities import *
from pyspark.sql.functions import col

df = spark.read.csv("bankcsv.csv")


class TestUtilities(unittest.TestCase):

    def test_readCsv(csvfile):
        result1 = spark.read.csv("bankcsv.csv").show(5)
        result2 = Utilities.readCsv("bankcsv.csv").show(5)
        assert result1 == result2
        
    def test_dropDotColumn(dataframe):
        result3 = Utilities.dropDotColumn(df).show(5)
        result4 = df.drop(".").show(5)
        assert result3 == result4


if __name__ == '__main__':
    unittest.main()

