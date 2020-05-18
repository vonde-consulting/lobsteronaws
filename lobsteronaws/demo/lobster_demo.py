import pandas as pd
from lobsteronaws.lobster.order_book import OrderBook
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

order_book_parquet_directory = "/home/ruihong/workspace/lobster/ruihong-testing-bucket/demo_pandas/orderbook"
order_books = pd.read_parquet(order_book_parquet_directory, "pyarrow")

# count the number of observation
order_books.groupby("symbol")["symbol"].count()

# Work on one stock data
symbol = "AAPL"
order_book_df = order_books.loc[order_books['symbol'] == symbol, :]

# release some memory
del order_books

aapl_order_book = OrderBook(order_book_df)
resample_interval_seconds = 60
resample_book = aapl_order_book.resample(resample_interval_seconds)
resample_book.loc[:, aapl_order_book.quote_cols].plot()

fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(resample_book.index.to_series(), resample_book.tradePrice / 1e4, label='Price')
ax.set_ylabel('Price')
ax2 = ax.twinx()
ax2.bar(resample_book.index.to_series(), height=resample_book.tradeSize, width=resample_interval_seconds / (3600 * 24),
        label='Volume', color='c')
ax2.set_ylabel('Volume')
ax.grid()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
