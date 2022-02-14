import logging
import os
import sys

from pyspark.sql.functions import to_date, when

logging. \
    basicConfig(filename="logFilePOCTwoOG.txt", filemode='w',
                format='%(asctime)s %(levelname)s-%(message)s')

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

logging.warning("Starting the utilities class")

spark = SparkSession.builder.appName('My POC Two App').getOrCreate()


class Utilities:

    def __int__(self):
        self.dataframe = None

    """
    readCsv() function reads a csv file from local storage and
    return a dataframe with the same schema. It takes only one input which
    is a csv file name.
    """

    # csv to dataframe
    def readCsv(csvfile):
        try:
            logging.warning("File is reading from Utility class")
            return spark.read.csv(csvfile, inferSchema=True, header=True)
        except:
            logging.error("File not found")

    """
    writeParquet() function reads a dataframe and saves the 
    content to a parquet file. It takes two input one is the dataframe 
    that we need to save and another is the parquet file name that is going 
    to store the dataframe data.
    """

    # Save dataframe to parquet
    def writeParquet(dataframe, parquetfile):
        try:
            dataframe.write.mode("overwrite").option("header", "true"). \
                partitionBy("Year", "Month").format("parquet").save(parquetfile)
            logging.warning("File is saving from Utility class")
        except:
            logging.error("File save error")

    """
    fixColumnNames() function removes all the white spaces from a column name and 
    replace them with underscore(_). It takes only the dataframe name as the input. It returns 
    the same dataframe as output but with updated column names.
    """

    # Fixing Column names, removing whitespaces
    def fixColumnNames(dataframe):
        new_column_name_list = list(map(lambda x: x.replace(" ", "_"), dataframe.columns))
        logging.warning("Column names are fixed, removed whitespaces")
        return dataframe.toDF(*new_column_name_list).withColumnRenamed("CHQ.NO.", "CHQ_NO")

    """
    dropDotColumn() function takes a dataframe as input and deletes the column which only have 
    dots in it. It return the updated dataframe as output.
    """

    # drop the column with name dot
    def dropDotColumn(dataframe):
        logging.warning("Dropped the column with dots")
        return dataframe.drop(".")

    """
    stringToDate() function takes a dataframe as input. It changes the type 
    of the column from String to Date format. Here the date format is 
    YYYY-MM-DD. It returns the updated dataframe as output.
    """

    # Change DATE column from string to date format
    def stringToDate(dataframe):
        logging.warning("Changed the DATE column format from sting to date")
        return dataframe.withColumn('DATE', to_date(dataframe.DATE, 'yyyy-MM-dd'))

    """
    addTransactionAmountColumn() function adds a new column named TransactionAmount 
    in the dataframe. It returns the updated dataframe.
    """

    # Adding new TransactionAmount Column
    def addTransactionAmountColumn(dataframe):
        logging.warning("Added the TransactionAmount column")
        return dataframe.withColumn \
            ('TransactionAmount', when(dataframe['WITHDRAWAL_AMT'].
                                       isNull(), dataframe['DEPOSIT_AMT']).
             when(dataframe['DEPOSIT_AMT'].isNull(), dataframe['WITHDRAWAL_AMT']).otherwise(0))

    """
    addTransactionTypeColumn() function adds a new column named TransactionType in 
    the dataframe. It returns the updated dataframe.
    """

    # Adding new TransactionType Column
    def addTransactionTypeColumn(dataframe):
        logging.warning("Added the TransactionType column")
        return dataframe.withColumn \
            ('TransactionType', when(dataframe.WITHDRAWAL_AMT.isNull(), "CR").
             when(dataframe.DEPOSIT_AMT.isNull(), "DR"))

    """
    formatColumnPositions() function set the column positions as required. 
    It takes a dataframe as input and returns the same dataframe but with the 
    updated column positions.
    """

    # Format the column positions
    def formatColumnPositions(dataframe):
        logging.warning("Formatted the column positions")
        return dataframe.select \
            (dataframe.Account_No, dataframe.DATE, dataframe.TRANSACTION_DETAILS,
             dataframe.CHQ_NO, dataframe.VALUE_DATE, dataframe.WITHDRAWAL_AMT,
             dataframe.DEPOSIT_AMT, dataframe.TransactionType, dataframe.TransactionAmount,
             dataframe.BALANCE_AMT)

    """
    dropDuplicateTransactions() function takes a dataframe as input. It deletes 
    all the duplicate transactions from the dataframe according to Account_No, 
    DATE, TransactionType and TransactionAmount column. It returns the 
    updated dataframe as output.
    """

    # Drop duplicate transactions
    def dropDuplicateTransactions(dataframe):
        logging.warning("Dropped the duplicate transactions")
        return dataframe. \
            dropDuplicates(["Account_No", "DATE", "TransactionType", "TransactionAmount"])


logging.warning("Ending the utilities class")
