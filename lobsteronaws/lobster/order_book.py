from lobsteronaws.lobster.functions import *


class OrderBook:
    def __init__(self, raw_data_frame):
        self.order_book = raw_data_frame
        self.order_book['timestamp'] = convert_timestamp(self.order_book['date'], self.order_book['time'])
        self.order_book.drop(['date', 'time'], inplace=True)
        self.order_book.set_index("timestamp", inplace=True)

    def resample(self, interval_seconds):
        _quote_cols = [s for s in self.order_book.columns if
                       (s.lower().contains("price") and not (s == "effectivePrice"))]
        _vol_cols = [s for s in self.order_book.columns if
                     (s.lower().contains("quantity") and not (s == "effectiveQuantity"))]
        _resample_book = self.order_book.resample(f"{interval_seconds}S")[_quote_cols + _vol_cols].pad()
        _trades = self.order_book.loc[
            self.order_book['eventType'] in ['Execution', 'HiddenOrderExecution'], ['effectivePrice',
                                                                                    'effectiveQuantity']] \
            .rename(columns={'effectivePrice': 'tradePrice', 'effectiveQuantity': 'tradeSize'})
        _resample_trades_group = _trades.resample[f'{interval_seconds}S']
        _resample_trade_price = _resample_trades_group['tradePrice'].pad()
        _resample_trade_size = _resample_trades_group['tradeSize'].sum()
        return pd.concat([_resample_book, _resample_trade_price, _resample_trade_size], axis=1)
