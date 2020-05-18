import pandas as pd
import numpy as np


def convert_timestamp(date, nanoseconds_from_mid_night):
    _time_delta = pd.to_timedelta(nanoseconds_from_mid_night, unit='ns')
    return pd.to_datetime(date.astype(str)) + _time_delta


def time_weighted_average(df, time_step):
    seconds = (df.index.minute * 60 + df.index.second) - time_step
    weight = [k / time_step if n == 0 else (seconds[n] - seconds[n - 1]) / time_step
              for n, k in enumerate(seconds)]
    return np.sum(weight * df.values)
