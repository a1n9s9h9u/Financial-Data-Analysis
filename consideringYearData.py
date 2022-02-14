import pocTwoRun
from pyspark.sql.functions import col, avg

df101 = pocTwoRun.dfCorporateNewWithAvg
df101.show()
df101.printSchema()

df201 = df101.where(((df101.Year == 2016) & (df101.Month <= 1)) | ((df101.Year == 2015) & (df101.Month >= 2)))

df301 = df201.groupBy(col("Account_No")).agg(avg("avgHighRiskTransactionAmount"))
df301.show()

def avgHighRiskTransactionAmountPerMonthConsideringPrevYearData(dataframe, year, month):
    dataframe = dataframe.\
        where(((dataframe.Year == year) & (dataframe.Month <= month)) | ((dataframe.Year == (year-1)) & (dataframe.Month >= (month+1))))
    dataframe = dataframe.groupBy(col("Account_No")).agg(avg("avgHighRiskTransactionAmount"))
    return dataframe

df401 = avgHighRiskTransactionAmountPerMonthConsideringPrevYearData(df101, 2016, 1)
df401.show()
