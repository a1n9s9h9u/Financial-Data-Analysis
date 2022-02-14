from pocTwoUtilities import *
from pyspark.sql import Window
from pyspark.sql.functions import col, year, month, sum, avg, date_format, mean

logging.warning("Starting the corporate class")


class Corporate(Utilities):

    def __init__(self):
        super().__init__()

    """
        totalTransactionAmountPerMonth() function takes a dataframe as input. It calculates the
        sum of all the transactions and groups it by the Account_No, Year and
        Month columns. It then sorts the dataframe according to the Year and Month
        columns. It returns the updated dataframe.
    """

    def totalTransactionAmountPerMonth(dataframe):
        logging.warning("Calculated the Total Transaction Amount per month")
        return dataframe.groupBy(col("Account_No"), year("DATE"), month("DATE")). \
            agg(sum("TransactionAmount")).sort("year(DATE)", "month(DATE)")

    """
        avgHighRiskTransactionAmountPerMonth() function takes a dataframe as input. 
        It calculates the average high risk transactions considering one month 
        data and groups it by the Account_No, Year and
        Month columns. It then returns the updated dataframe.
    """

    def avgHighRiskTransactionAmountPerMonth(dataframe):
        logging.warning("Calculated the Average High Risk Transaction Amount per month")
        return dataframe.groupBy(col("Account_No"), col("Year"), col("Month")). \
            agg(avg("TotalHighRiskTransactionAmount"))

    """
        avgHighRiskTransactionAmountPerMonthConsideringPrevYearData() function 
        takes a dataframe as input. It calculates the average high 
        risk transactions considering previous one year data. 
        Here we created separate window of 365 days for each month 
        to consider the previous one year data. 
        It then returns the updated dataframe.
    """

    def avgHighRiskTransactionAmountPerMonthConsideringPrevYearData(dataframe):
        dataframe = dataframe. \
            withColumn("RiskStatus", when(col("TRANSACTION_DETAILS").
                                          contains("Indiaforensic"), "High Risk"))
        highrisk = dataframe.filter(dataframe.RiskStatus == "High Risk")
        days = lambda i: i * 86400
        w = (Window().partitionBy(col("Account_No")).
             orderBy(col("MonthFirst").cast("timestamp").cast("long")).
             rangeBetween(-days(365), 0))
        avg_high_risk = highrisk. \
            select(col("Account_No"), (date_format('DATE', "yyyy").alias("Year")),
                   (date_format('DATE', "MM").
                    alias("Month")), mean("TransactionAmount").
                   over(w).alias("AvgHighRiskTransaction")).distinct()
        return avg_high_risk

    """
        getDataForAMonth() function takes a dataframe as input. It 
        also takes a year and a month as input too. It then filters out the 
        data according to the the Year and 
        Month columns. It returns the data for a particular month and 
        returns it as a new dataframe.
    """

    def getDataForAMonth(dataframe, year, month):
        logging.warning("Getting the final Corporate data per month")
        return dataframe.filter((dataframe["Year"] == year) & (dataframe["Month"] == month))


df = Utilities.readCsv("bankcsv.csv")
df = Utilities.fixColumnNames(df)
df = Utilities.dropDotColumn(df)
df = Utilities.stringToDate(df)
df = Utilities.addTransactionTypeColumn(df)
df = Utilities.addTransactionAmountColumn(df)
df = Utilities.formatColumnPositions(df)
df = Utilities.dropDuplicateTransactions(df)

dfCorporate = df.filter(df["Account_No"].like("1%"))
dfCorporate = dfCorporate.withColumn("MonthFirst", month("DATE"))
# dfCorporate.show()


logging.warning("Ending the corporate class")
