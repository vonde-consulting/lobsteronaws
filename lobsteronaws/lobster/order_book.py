from lobsteronaws.lobster.functions import *


class OrderBook:
    def __init__(self, raw_data_frame):
        self.order_book = raw_data_frame
        self.order_book['timestamp'] = convert_timestamp(self.order_book['date'], self.order_book['time'])
        self.order_book.drop(columns=['date', 'time'], inplace=True)
        self.order_book.set_index("timestamp", inplace=True)
        self.quote_cols = [s for s in self.order_book.columns if
                           (('price' in s.lower()) and not (s == "effectivePrice"))]
        self.vol_cols = [s for s in self.order_book.columns if
                         (('quantity' in s.lower()) and not (s == "effectiveQuantity"))]

    def resample(self, interval_seconds):
        _resample_book = self.order_book.resample(f"{interval_seconds}S")[
            self.quote_cols + self.vol_cols].last().ffill()
        _trades = self.order_book.loc[
            self.order_book['eventType'].apply(lambda x: x in ['Execution', 'HiddenOrderExecution']), ['effectivePrice',
                                                                                                       'effectiveQuantity']] \
            .rename(columns={'effectivePrice': 'tradePrice', 'effectiveQuantity': 'tradeSize'})
        _resample_trades_group = _trades.resample(f'{interval_seconds}S')
        _resample_trade_price = _resample_trades_group['tradePrice'].last().ffill()
        _resample_trade_size = _resample_trades_group['tradeSize'].sum().fillna(0)
        return pd.concat([_resample_book, _resample_trade_price, _resample_trade_size], axis=1)
