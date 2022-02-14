from pocTwoUtilities import *
from pocTwoRetail import *
from pocTwoCorporate import *
from accountName import *
from pyspark.sql.functions import col

logging.warning("Starting the pipeline")

RetailSumDeposit = Retail.sumDepositPerMonth(dfRetail). \
    withColumnRenamed("year(DATE)", "YearDeposit"). \
    withColumnRenamed("month(DATE)", "MonthDeposit"). \
    withColumnRenamed("sum(DEPOSIT_AMT)", "TotalDeposit"). \
    withColumnRenamed("Account_No", "Account_No_Deposit")

logging.warning("Getting the final Corporate data per month")

RetailSumDeposit = RetailSumDeposit.fillna(0)

RetailSumWithdrawal = Retail.sumWithdrawalPerMonth(dfRetail). \
    withColumnRenamed("year(DATE)", "YearWithdrawal"). \
    withColumnRenamed("month(DATE)", "MonthWithdrawal"). \
    withColumnRenamed("sum(WITHDRAWAL_AMT)", "TotalWithdrawal"). \
    withColumnRenamed("Account_No", "Account_No_Withdrawal")

RetailSumWithdrawal = RetailSumWithdrawal.fillna(0)

RetailSumDepositWithdrawal = RetailSumDeposit. \
    join(RetailSumWithdrawal,
         ((RetailSumDeposit["YearDeposit"] == RetailSumWithdrawal["YearWithdrawal"]) &
          (RetailSumDeposit["MonthDeposit"] ==
           RetailSumWithdrawal["MonthWithdrawal"]) &
          (RetailSumDeposit["Account_No_Deposit"] ==
           RetailSumWithdrawal[
               "Account_No_Withdrawal"])), 'inner')

RetailSumDepositWithdrawal = RetailSumDepositWithdrawal. \
    select("Account_No_Deposit", "YearDeposit", "MonthDeposit",
           "TotalDeposit", "TotalWithdrawal"). \
    withColumnRenamed("YearDeposit", "Year"). \
    withColumnRenamed("MonthDeposit", "Month"). \
    withColumnRenamed("Account_No_Deposit", "Account_No").\
    sort("Year", "Month")

RetailSumDepositWithdrawal = RetailSumDepositWithdrawal. \
    withColumn("EndBalancePerMonth", col("TotalDeposit") - col("TotalWithdrawal"))

RetailSumDepositWithdrawalWithAccountName = RetailSumDepositWithdrawal. \
    join(RetailAccountNameColumn,
         (RetailSumDepositWithdrawal["Account_No"] ==
          RetailAccountNameColumn["Account_No_New"]), 'inner')

RetailSumDepositWithdrawalWithAccountName = RetailSumDepositWithdrawalWithAccountName \
    .select("Account_No", "Account_Name", "Year", "Month",
            "TotalDeposit", "TotalWithdrawal", "EndBalancePerMonth")

dfCorporateSumTransactions = Corporate. \
    totalTransactionAmountPerMonth(dfCorporate)

dfCorporateSumTransactions = dfCorporateSumTransactions. \
    withColumnRenamed("year(DATE)", "Year"). \
    withColumnRenamed("month(DATE)", "Month"). \
    withColumnRenamed("sum(TransactionAmount)", "TotalTransaction")

CorporateSumTransactionsWithAccountName = dfCorporateSumTransactions. \
    join(CorporateAccountNameColumn,
         (dfCorporateSumTransactions["Account_No"] ==
          CorporateAccountNameColumn["Account_No_New"]), 'inner')

CorporateSumTransactionsWithAccountName = CorporateSumTransactionsWithAccountName \
    .select("Account_No", "Account_Name", "Year", "Month", "TotalTransaction")

dfCorporateNew = dfCorporate. \
    filter(dfCorporate["TRANSACTION_DETAILS"].contains("Indiaforensic"))

dfCorporateNew = Corporate. \
    totalTransactionAmountPerMonth(dfCorporateNew)

dfCorporateNew = dfCorporateNew.withColumnRenamed("year(DATE)", "Year"). \
    withColumnRenamed("month(DATE)", "Month"). \
    withColumnRenamed("sum(TransactionAmount)", "TotalHighRiskTransactionAmount")
# dfCorporateNew.show() # Showing total high risk transaction amount per month

dfCorporateNewWithAvg = Corporate. \
    avgHighRiskTransactionAmountPerMonth(dfCorporateNew)
dfCorporateNewWithAvg = dfCorporateNewWithAvg. \
    withColumnRenamed("avg(TotalHighRiskTransactionAmount)",
                      "avgHighRiskTransactionAmount")

# Final Retail Dataframe
# RetailSumDepositWithdrawalWithAccountName.show()

# Save per month Retail data to a parquet file
Utilities.writeParquet(RetailSumDepositWithdrawalWithAccountName, "Retail")

FinalCorporate = Corporate.\
    avgHighRiskTransactionAmountPerMonthConsideringPrevYearData(dfCorporate)

# Final Corporate Dataframe
# FinalCorporate.show()

# Save per month Retail data to a parquet file
Utilities.writeParquet(FinalCorporate, "Corporate")

logging.warning("Ending the pipeline")
