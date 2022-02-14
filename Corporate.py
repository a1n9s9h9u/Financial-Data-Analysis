import logging
import os
import sys

from Utility import UtilityNew
from pyspark.sql.functions import col, year, month, when, date_format, mean
from pyspark.sql.window import Window
from accountName import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

logging.warning("Starting the retail class")


class CorporateNew(UtilityNew):
    def __init__(self, file):
        super().__init__()
        self.data = super().csvRead(file)
        self.data = super().stringToDate(self.data)
        self.data = self.addCorporateName()
        self.data = self.data.filter(self.data["Account_No"].like("1%"))

    def totalTransactionAmountPerMonth(self):
        logging.warning("Calculated the Total Transaction Amount per month")
        return self.data.groupBy(col("Account_No"), year("DATE"), month("DATE")). \
            sum("TransactionAmount").withColumnRenamed("year(DATE)", "Year").\
            withColumnRenamed("month(DATE)", "Month").\
            withColumnRenamed("sum(TransactionAmount)", "Total_Transaction_Amount")

    def avgHighRiskTransactionAmountPerMonthConsideringPrevYearData(self):
        self.data = self.data. \
            withColumn("RiskStatus", when(col("TRANSACTION_DETAILS").
                                          contains("Indiaforensic"), "High Risk"))
        highrisk = self.data.filter(self.data.RiskStatus == "High Risk")
        days = lambda i: i * 86400
        w = (Window().partitionBy(col("Account_No")).
             orderBy(col("DATE").cast("timestamp").cast("long")).
             rangeBetween(-days(365), 0))
        avg_high_risk = highrisk. \
            select(col("Account_No"), (date_format('DATE', "yyyy").alias("Year")),
                   (date_format('DATE', "MM").
                    alias("Month")), mean("TransactionAmount").
                   over(w).alias("AvgHighRiskTransaction")).distinct()
        return avg_high_risk

    def addCorporateName(self):
        return self.data.join(CorporateAccountNameColumn,
             (self.data["Account_No"] ==
              CorporateAccountNameColumn["Account_No_New"]), 'inner')


logging.warning("Ending the corporate class")

"""
myCorporate = CorporateNew("updatedbank")
mydf1 = myCorporate.totalTransactionAmountPerMonth()
mydf1.show()
mydf2 = myCorporate.avgHighRiskTransactionAmountPerMonthConsideringPrevYearData()
mydf2.show(500)
mydf3 = myCorporate.addCorporateName()
mydf3.show()
"""
