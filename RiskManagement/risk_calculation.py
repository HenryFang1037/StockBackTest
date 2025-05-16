import pandas as pd
import numpy as np


def calc_return(df: pd.DataFrame, buy_day: str, sell_day: str):
    """
    持仓收益波动
    :param df:
    :param buy_day:
    :param sell_day:
    :return:
    """
    series = pd.Series()
    buy_id = df[df['日期']==buy_day].index.to_list()[0]
    sell_id = df[df['日期']==sell_day].index.to_list()[0]
    if buy_id < sell_id < df.index.stop:
        buy_price = df.loc[buy_id].收盘
        series = df.loc[buy_id + 1:]['收盘'] / buy_price - 1
    return series


def annualized_ret(data: pd.Series):
    """
    年化收益率
    :param data:
    :return:
    """
    days = data.shape[0]
    return np.power(data.iloc[-1], 250 / days)


def volatility(data: pd.Series):
    """
    收益波动率
    :param data:
    :return:
    """
    return data.std()


def downside_volatility(data: pd.Series):
    """
    下行收益波动率
    :param data:
    :return:
    """
    ret = data[data < 0]
    return ret.std()


def max_dropdown(data: pd.Series):
    """
    最大回撤
    :param data:
    :return:
    """
    data = data + 1
    max_id = data.idxmax()
    max_return= data.loc[max_id]
    min_return = data.loc[max_id:].min()
    return min_return / max_return - 1


def system_risk(ret: pd.Series, base_line_ret: pd.Series):
    """
    系统性风险（收益）
    :param ret:
    :param base_line_ret:
    :return:
    """
    covariance = np.cov(ret, base_line_ret)
    beta = covariance / (ret.std() * base_line_ret.std())
    return beta


def none_system_risk(beta: float, ret: pd.Series, base_line_ret: pd.Series, national_bond_ret: float):
    """
    非系统性风险（收益）
    :param beta:
    :param ret:
    :param base_line_ret:
    :param national_bond_ret:
    :return:
    """
    ret = annualized_ret(ret)
    base_line_ret = annualized_ret(base_line_ret)
    alpha = ret - national_bond_ret - beta * (base_line_ret - national_bond_ret)
    return alpha


def sharp_ratio(ret: pd.Series, national_bond_ret: float):
    """
    夏普比率
    :param ret:
    :param national_bond_ret:
    :return:
    """
    ret = annualized_ret(ret)
    var = volatility(ret)
    ratio = (ret - national_bond_ret) / var
    return ratio


def sortino_ratio(ret: pd.Series, national_bond_ret: float):
    """
    索提诺比率
    :param ret:
    :param national_bond_ret:
    :return:
    """
    ret = annualized_ret(ret)
    downside_vol = downside_volatility(ret)
    ratio = (ret - national_bond_ret) / downside_vol
    return ratio


def calc_all(stock_code: str, buy_day: str, sell_day: str, base_line_code='sh000001', national_bond_code="中债国债收益率曲线"):
    pass


if __name__ == '__main__':
    import akshare as ak

    bond_china_yield_df = ak.bond_china_yield(start_date="20250201", end_date="20250501")
    print(bond_china_yield_df)













