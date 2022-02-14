import unittest
import os.path

from Utility import *
from test_Base import *

utilityNewTestObj = UtilityNew()
df = utilityNewTestObj.csvRead("dummy.csv")
utilityNewTestObj.parquetWrite(df, "testParquetFile")
dfDateFormat = utilityNewTestObj.stringToDate(df)
# dfDateFormat.printSchema()
# dfDateFormat.write.mode("overwrite").options(header=True).csv("DfDateFormat")


class TestUtilityNew(unittest.TestCase):

    # def test_csvRead(self):
        # self.assertEqual(df, dummydf.show())

    def test_csvRead(self):
        self.assertEqual(df.subtract(dummydf).rdd.count(), 0)

    def test_parquetWrite(self):
        self.assertEqual(os.path.exists('testParquetFile'), True)


if __name__ == '__main__':
    unittest.main()

