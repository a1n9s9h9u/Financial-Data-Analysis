from pocTwoUtilities import *

data = [("409000611074'", "R1"), ("409000493201'", "R2"),
        ("409000425051'", "R3"), ("409000405747'", "R4"), ("409000438611'", "R5"),
        ("409000493210'", "R6"), ("409000438620'", "R7"), ("409000362497'", "R8")]
columns = ["Account_No_New", "Account_Name"]

RetailAccountNameColumn = spark.createDataFrame(data=data, schema=columns)
# RetailAccountNameColumn.show()

dataCorporate = [("1196711'", "C1"), ("1196428'", "C2")]
columnsCorporate = ["Account_No_New", "Account_Name"]

CorporateAccountNameColumn = spark.\
    createDataFrame(data=dataCorporate, schema=columnsCorporate)
# CorporateAccountNameColumn.show()
