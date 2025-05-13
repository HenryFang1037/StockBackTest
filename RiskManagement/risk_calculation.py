from datetime import datetime
import itertools
import pandas as pd
import numpy as np


def calc_return(df: pd.DataFrame, buy_idx: list, sell_idx: list):
    def calc_once(data, b_id):
        b_day = data.iloc[b_id].日期
        b_price = data.iloc[b_id].收盘
        return_list = (data.iloc[b_id + 1:]['收盘'] - b_price) * np.floor(100000 / b_price)
        max_return = round(return_list.max(), 2)
        min_return = round(return_list.min(), 2)
        hold_return = round(return_list.iloc[-1], 2)
        h_days = datetime.strptime(data.iloc[-1].日期, '%Y-%m-%d') - datetime.strptime(b_day, '%Y-%m-%d')
        return {
            'MaxReturn': max_return,
            'MinReturn': min_return,
            'HoldReturn': hold_return,
            'HoldDays': h_days.days
        }

    result = {
        'MaxReturn': 0,
        'MinReturn': 0,
        'HoldReturn': 0,
        'HoldDays': 0,
    }

    if len(buy_idx) == 0:
        return result
    if len(sell_idx) == 0:
        if len(buy_idx) > 1:
            return result
        else:
            calc_once(df, buy_idx[0])

    returns = []
    hold_days = []
    for buy_id, sell_id in itertools.zip_longest(buy_idx, sell_idx, fillvalue=None):
        if buy_id is not None and sell_id is not None and buy_id < sell_id != df.last_valid_index:
            buy_day = df.iloc[buy_id].日期
            sell_day = df.iloc[sell_id].日期
            days = datetime.strptime(sell_day, '%Y-%m-%d') - datetime.strptime(buy_day, '%Y-%m-%d')
            hold_days.append(days.days)
            buy_price = df.iloc[buy_id].收盘
            sell_price = df.iloc[sell_id].收盘
            ret = (sell_price - buy_price) * np.floor(100000 / buy_price)
            returns.append(ret)
        elif buy_id is not None and sell_id is None and buy_id != df.last_valid_index:
            result = calc_once(df, buy_id)
            result['MaxReturn'] = max(result['MaxReturn'], max(returns))
            result['MinReturn'] = min(result['MinReturn'], min(returns))
            result['HoldDays'] = result['HoldDays'] + sum(hold_days)
            result['HoldReturn'] = result['HoldReturn'] + sum(returns)
            return result
        else:
            return {
                    'MaxReturn': max(returns),
                    'MinReturn': min(returns),
                    'HoldReturn': sum(returns),
                    'HoldDays': sum(hold_days)
                }




