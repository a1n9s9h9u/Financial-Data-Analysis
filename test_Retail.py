import unittest

from Retail import RetailNew
from Utility import UtilityNew
from test_Base import *

utilityNewTestObj = UtilityNew()
retailNewTestObj = RetailNew("dummy.csv")
sumDeposit = retailNewTestObj.sumDepositPerMonth()
sumDeposit.show()


class TestRetailNew(unittest.TestCase):

    def test_sumDepositPerMonth(self):
        pass


if __name__ == '__main__':
    unittest.main()

