import logging
import os
import sys

from Utility import UtilityNew
from pyspark.sql.functions import col, year, month
from accountName import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

logging.warning("Starting the retail class")


class RetailNew(UtilityNew):

    def __init__(self, file):
        super().__init__()
        self.data = super().csvRead(file)
        self.data = super().stringToDate(self.data)
        self.data = self.addRetailName()
        self.data = self.data.filter(self.data["Account_No"].like("4%"))

    def sumDepositPerMonth(self):
        logging.warning("Calculated the Sum Deposit per month")
        return self.data.groupBy(col("Account_No"), year("DATE"), month("DATE")).\
            sum("DEPOSIT_AMT").withColumnRenamed("year(DATE)", "Year").\
            withColumnRenamed("month(DATE)", "Month").\
            withColumnRenamed("sum(DEPOSIT_AMT)", "Total_Deposits")

    def sumWithdrawalPerMonth(self):
        logging.warning("Calculated the Sum Withdrawal per month")
        return self.data.groupBy(col("Account_No"), year("DATE"), month("DATE")).\
            sum("WITHDRAWAL_AMT").withColumnRenamed("year(DATE)", "Year").\
            withColumnRenamed("month(DATE)", "Month").\
            withColumnRenamed("sum(WITHDRAWAL_AMT)", "Total_Withdrawal")

    def balanceAmtPerMonth(self):
        logging.warning("Calculated the Balance Amount (Deposit - Withdrawal) per month")
        dfStage = self.data.groupBy(col("Account_No"), year("DATE"), month("DATE")).\
            sum("DEPOSIT_AMT", "WITHDRAWAL_AMT").withColumnRenamed("year(DATE)", "Year").\
            withColumnRenamed("month(DATE)", "Month").\
            withColumnRenamed("sum(DEPOSIT_AMT)", "Total_Deposits").\
            withColumnRenamed("sum(WITHDRAWAL_AMT)", "Total_Withdrawal").fillna(0)
        return dfStage.\
            withColumn('EndBalancePerMonth', col('Total_Deposits') - col('Total_Withdrawal'))

    def addRetailName(self):
        return self.data.join(RetailAccountNameColumn,
             (self.data["Account_No"] ==
              RetailAccountNameColumn["Account_No_New"]), 'inner')


logging.warning("Ending the retail class")


# myRetail = RetailNew("updatedbank")
# mydf1 = myRetail.sumDepositPerMonth()
# mydf1.show()
# mydf2 = myRetail.sumWithdrawalPerMonth()
# mydf2.show()
# mydf3 = myRetail.balanceAmtPerMonth()
# mydf3.show()
# mydf4 = myRetail.addRetailName()
# mydf4.show()

