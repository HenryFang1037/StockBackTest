import warnings

import numpy as np
import pandas as pd
from datetime import datetime

from RiskManagement.risk_calculation import calc_return

warnings.filterwarnings('ignore')


def gap_finder(df: pd.DataFrame, search_window: int=80, search_step: int=1, least_rise_day: int = 5, least_drop_day: int = 3,
               max_small_turnover_multiply: float = 0.2, least_increase: float = 0.6,
               least_decrease: float = -0.4) -> list:
    """
    This function searches for gaps in stock data and provides buying recommendations.

    Parameters
    ----------
    search_window : int
    search_step : int
    df : pd.DataFrame
        The stock data.
    least_rise_day : int, optional
        The minimum number of days required for the stock to rise from its opening price to its highest price. The default is 5.
    least_drop_day : int, optional
        The minimum number of days required for the stock to fall from its highest price to its opening price. The default is 3.
    max_small_turnover_multiply : float, optional
        The maximum allowed trading volume to price ratio. If the trading volume is smaller than the maximum allowed ratio, the stock is considered to be in a bullish trend. The default is 0.2.
    least_increase : float, optional
        The minimum allowed increase in the stock price. The default is 0.6.
    least_decrease : float, optional
        The minimum allowed decrease in the stock price. The default is -0.5.

    Returns
    -------
    str
        A buying recommendation.

    Raises
    ------
    ValueError
        If the input data is not a pandas DataFrame.

    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("The input data must be a pandas DataFrame.")

    stock_name = df['name'].unique()[0]
    stock_code = df['code'].unique()[0]

    comment = {
        'info':  '股票：{}, 代码：{}',
        'first': '1.从{}开盘到{}最高点共{}日，涨幅达到{:.2%}，从最高点到{}日开盘，共{}日，跌幅达到{:.2%}。',
        'second': '2.从最高点到昨日共形成{}个缺口，分别是{}。',
        'third': '3.在{}日收盘价为{}，交易量缩小到最大交易量换手率的{}倍，建议后续可买入，持有到达到{}价格。'
    }

    def gap_search(data):
        highest_high_idx = data['最高'].idxmax()
        if highest_high_idx > data.index[-1] - least_drop_day:
            return None

        highest_high = data['最高'].loc[highest_high_idx]
        highest_turnover = data['换手率'].loc[highest_high_idx - 2:highest_high_idx + 3].mean()
        highest_high_day = data['日期'].loc[highest_high_idx]
        max_small_turnover = highest_turnover * max_small_turnover_multiply
        left_data = data.loc[:highest_high_idx + 1]
        lowest_open_idx = left_data['开盘'].idxmin()
        lowest_open = left_data['开盘'].loc[lowest_open_idx]
        lowest_open_day = left_data['日期'].loc[lowest_open_idx]
        max_increase = round(highest_high / lowest_open - 1, 4)
        increase_day = highest_high_idx - lowest_open_idx

        if max_increase >= least_increase and increase_day >= least_rise_day:
            right_data = data.loc[highest_high_idx:]
            right_data['gap'] = right_data['收盘'].shift(1) > right_data['最高']
            if np.any(right_data['gap']):
                indexes = []
                gap_indexes = right_data[right_data['gap']==True].index.values
                for gap_index in gap_indexes:
                    pre_close = data['收盘'].loc[gap_index - 1]
                    indicator = pre_close > right_data['最高'].loc[gap_index:]
                    if all(indicator):
                        indexes.append(gap_index)
                gap_number = len(indexes)
                if gap_number != 0:
                    latest_gap_index = indexes[-1]
                    gap_days = [str(date) for date in right_data['日期'].loc[indexes]]
                    gap_previous_days_idxes = [idx -1 for idx in indexes]
                    gap_close_prices = list(data['收盘'].loc[gap_previous_days_idxes])

                    for i, idx in enumerate(range(latest_gap_index + 1, right_data.index.stop), 1):
                        turnover = data['换手率'].loc[idx - 1:idx + 2].mean()
                        dropday = idx - highest_high_idx
                        close_price = right_data['收盘'].loc[idx]
                        pre_close_price = right_data['收盘'].loc[idx - 1]
                        pct = close_price / pre_close_price - 1
                        max_decrease = round(close_price / highest_high - 1, 4)
                        buy_day = right_data['日期'].loc[idx]
                        if max_decrease <= least_decrease and dropday >= least_drop_day and turnover <= max_small_turnover and pct >= -0.01 and idx < right_data.index.stop - 3:
                            sell_day = right_data['日期'].loc[right_data.index.stop-1]
                            comment['info'] = comment['info'].format(stock_name, stock_code)
                            comment['first'] = comment['first'].format(lowest_open_day, highest_high_day, increase_day, max_increase, buy_day, dropday, max_decrease)
                            comment['second'] = comment['second'].format(gap_number, list(reversed(gap_days)))
                            comment['third'] = comment['third'].format(buy_day, close_price, max_small_turnover_multiply, list(reversed(gap_close_prices)))

                            return_risk = calc_return(stock_code, data, buy_day, sell_day)

                            return {
                                'stock_code': stock_code,
                                'start_date': lowest_open_day,
                                'end_date': None,
                                'buy_date': buy_day,
                                'sell_date': sell_day,
                                'return': return_risk,
                                'comment': comment,
                                'test_date': datetime.now().strftime('%Y-%m-%d')
                            }

    datas = {}
    for i in range(0, df.shape[0]-search_window+search_step, search_step):
        data = gap_search(df.loc[i:i+search_window])
        if data is not None:
            symbol = data['stock_code'] + '_' + data['buy_date']
            if symbol not in datas.keys():
                datas[symbol] = data
            else:
                datas[symbol].update(data)
    return datas


if __name__ == '__main__':
    from tqdm import tqdm
    results = {}

    from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB
    df = MongoDB('沪深A股日度数据').find('300251', start_date='2024-11-01', end_date='2025-05-06')
    df['name'] = '300251'
    df['code'] = '300251'
    res = gap_finder(df)
    print(res)


