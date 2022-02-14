from pocTwoUtilities import *
from pyspark.sql.functions import month, year, sum, col

logging.warning("Starting the retail class")


class Retail(Utilities):

    def __init__(self):
        super().__init__()

    """
    sumDepositPerMonth() function takes a dataframe as input. It calculates the
    sum of all the deposit transactions and groups it by the Account_No, Year and
    Month columns. It then sorts the dataframe according to the Year and Month
    columns. It returns the updated dataframe.
    """

    def sumDepositPerMonth(dataframe):
        logging.warning("Calculated the Sum Deposit per month")
        return dataframe.groupBy(col("Account_No"), year("DATE"), month("DATE")). \
            agg(sum("DEPOSIT_AMT")).sort("year(DATE)", "month(DATE)")

    """
        sumWithdrawalPerMonth() function takes a dataframe as input. It calculates the 
        sum of all the Withdrawal transactions and groups it by the Account_No, Year and 
        Month columns. It then sorts the dataframe according to the Year and Month 
        columns. It returns the updated dataframe.
    """

    def sumWithdrawalPerMonth(dataframe):
        logging.warning("Calculated the Sum Withdrawal per month")
        return dataframe.groupBy(col("Account_No"), year("DATE"), month("DATE")). \
            agg(sum("WITHDRAWAL_AMT")).sort("year(DATE)", "month(DATE)")

    """
        getDataForAMonth() function takes a dataframe as input. It 
        also takes a year and a month as input too. It then filters out the 
        data according to the the Year and 
        Month columns. It returns the data for a particular month and 
        returns it as a new dataframe.
    """

    def getDataForAMonth(dataframe, year, month):
        logging.warning("Getting the final Retail data per month")
        return dataframe. \
            filter((dataframe["Year"] == year) & (dataframe["Month"] == month))


df = Utilities.readCsv("bankcsv.csv")
df = Utilities.fixColumnNames(df)
df = Utilities.dropDotColumn(df)
df = Utilities.stringToDate(df)
df = Utilities.addTransactionTypeColumn(df)
df = Utilities.addTransactionAmountColumn(df)
df = Utilities.formatColumnPositions(df)
df = Utilities.dropDuplicateTransactions(df)

dfRetail = df.filter(df["Account_No"].like("4%"))
# dfRetail.show()


logging.warning("Ending the retail class")
