import logging
import os
import sys

from Corporate import CorporateNew
from Retail import RetailNew
from Utility import UtilityNew

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

logging.warning("Starting the pipeline")

utilityObject = UtilityNew()
retailObject = RetailNew("updatedbank")
corporateObject = CorporateNew("updatedbank")

finalRetail = retailObject.balanceAmtPerMonth()
utilityObject.parquetWrite(finalRetail, "Retail")

finalCorporate = corporateObject.\
    avgHighRiskTransactionAmountPerMonthConsideringPrevYearData()
utilityObject.parquetWrite(finalCorporate, "Corporate")

logging.warning("Ending the pipeline")
