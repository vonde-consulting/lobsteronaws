import unittest
from lobsteronaws.lobster.order_book import OrderBook
import pandas as pd
import os


class TestOrderBook(unittest.TestCase):

    def setUp(self):
        _parquet_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "resources", "adbe.parquet")
        self._order_book = OrderBook(pd.read_parquet(_parquet_file))

    # test method
    def test_resample(self):
        _resample_book = self._order_book.resample(300)
        self.assertEqual(_resample_book.shape[0], 78, "Number of resample rows")
        self.assertEqual(_resample_book.first_valid_index(), pd.Timestamp('2019-12-30 09:30:00', freq='300S'),
                         "First time index")
        self.assertEqual(_resample_book.last_valid_index(), pd.Timestamp('2019-12-30 15:55:00', freq='300S'),
                         "Last time index")


if __name__ == '__main__':
    unittest.main()
