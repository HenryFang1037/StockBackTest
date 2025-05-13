from datetime import datetime
import itertools
import pandas as pd


def calc_return(df: pd.DataFrame, buy_idx: list, sell_idx: list):
    def calc_once(df, buy_id):
        buy_day = df.iloc[buy_id].日期
        buy_price = df.iloc[buy_id].收盘
        returns = df.iloc[buy_id + 1:]['收盘'] / buy_price - 1
        max_return = round(returns.max() * 100, 2)
        min_return = round(returns.min() * 100, 2)
        hold_return = round(returns.iloc[-1] * 100, 2)
        hold_days = datetime.strptime(df.iloc[-1].日期, '%Y-%m-%d') - datetime.strptime(buy_day, '%Y-%m-%d')
        return {
            'MaxReturn': max_return,
            'MinReturn': min_return,
            'HoldReturn': hold_return,
            'HoldDays': hold_days
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
    for buy_id, sell_id in itertools.zip_longest(buy_idx, sell_idx, fillvalue=None):
        if buy_id is not None and sell_id is not None and buy_id < sell_id and sell_id != df.last_valid_index:
            buy_price = df.iloc[buy_id].收盘
            sell_price = df.iloc[sell_id].收盘
            ret = round((sell_price / buy_price - 1) * 100, 2)
            returns.append(ret)
        elif buy_id is not None and sell_id is None and buy_id != df.last_valid_index:
            result = calc_once(df, buy_id)
            result['MaxReturn'] = max(result['MaxReturn'], max(returns))
            result['MinReturn'] = min(result['MinReturn'], min(returns))
            return result
        else:
            return {
                    'MaxReturn': max(returns),
                    'MinReturn': min(returns),
                    'HoldReturn': 0,
                    'HoldDays': 0,
                }




