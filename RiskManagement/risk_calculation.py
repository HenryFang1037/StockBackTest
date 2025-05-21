import pandas as pd
import numpy as np
from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB


def calc_return(stock_code: str, df: pd.DataFrame, buy_day: str, sell_day: str):
    """
    持仓收益波动
    :param stock_code:
    :param df:
    :param buy_day:
    :param sell_day:
    :return:
    """
    result = {
        '年化收益率': 0,
        '收益波动率': 0,
        '收益下行波动率': 0,
        '收益最大回撤': 0,
        '系统性风险': 0,
        '非系统性风险': 0,
        '夏普比率': 0,
        '索提诺比率': 0
    }
    if stock_code.startswith('688'):
        index_symbol = 'sh000688'
    elif stock_code.startswith('300'):
        index_symbol = 'sz399006'
    elif stock_code.startswith('60'):
        index_symbol = 'sh000001'
    else:
        index_symbol = 'sz399001'
    index_close = MongoDB('沪深指数日度数据').find(index_symbol, start_date=buy_day, end_date=sell_day)['收盘']
    bond_data = MongoDB('债券日度数据').find('bond', start_date=buy_day, end_date=sell_day)
    bond_yield = bond_data[bond_data['曲线名称'] == '中债国债收益率曲线']['1年'].iloc[-1]
    buy_id = df[df['日期'] == buy_day].index.to_list()[0]
    sell_id = df[df['日期'] == sell_day].index.to_list()[0]
    if buy_id < sell_id < df.index.stop:
        buy_price = df.loc[buy_id].收盘
        index_price = index_close.iloc[0]
        stock_earning = df.loc[buy_id + 1:]['收盘'] / buy_price - 1
        index_earning = index_close.iloc[1:] / index_price - 1
        annualized_return = annualized_ret(stock_earning)
        vol = volatility(stock_earning)
        downside_vol = downside_volatility(stock_earning)
        dropdown = max_dropdown(stock_earning)
        beta = systematic_risk(stock_earning, index_earning)
        alpha = unsystematic_risk(beta, stock_earning, index_earning, bond_yield)
        sharp = sharp_ratio(stock_earning, bond_yield)
        sortino = sortino_ratio(stock_earning, bond_yield)
        result = {
            '收益率': round(stock_earning.iloc[-1], 4),
            '年化收益率': annualized_return,
            '收益波动率': vol,
            '收益下行波动率': downside_vol,
            '收益最大回撤': dropdown,
            '系统性风险': beta,
            '非系统性风险': alpha,
            '夏普比率': sharp,
            '索提诺比率': sortino
        }
    return result


def annualized_ret(data: pd.Series):
    """
    年化收益率
    :param data:
    :return:
    """
    days = data.shape[0]
    return round(np.power(1 + data.iloc[-1], 250 / days) - 1, 4)


def volatility(data: pd.Series):
    """
    收益波动率
    :param data:
    :return:
    """
    vol = 0 if data.empty else data.std()
    return round(vol, 4)


def downside_volatility(data: pd.Series):
    """
    下行收益波动率
    :param data:
    :return:
    """
    ret = data[data < 0]
    vol = 0 if ret.empty else ret.std()
    return round(vol, 4)


def max_dropdown(data: pd.Series):
    """
    最大回撤
    :param data:
    :return:
    """
    max_pct = data.iloc[0]
    max_dropdown = 0.0
    for p in data:
        max_pct = p if p > max_pct else max_pct
        current_dropdown = 1 - (p + 1) / (max_pct + 1)
        max_dropdown = current_dropdown if current_dropdown > max_dropdown else max_pct
    return round(max_dropdown, 4)


def systematic_risk(ret: pd.Series, base_line_ret: pd.Series):
    """
    系统性风险（收益）
    :param ret:
    :param base_line_ret:
    :return:
    """
    covariance = np.cov(ret, base_line_ret)
    beta = covariance[0][1] / (ret.std() * base_line_ret.std())
    return round(beta, 4)


def unsystematic_risk(beta: float, ret: pd.Series, base_line_ret: pd.Series, national_bond_ret: float):
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
    alpha = ret - national_bond_ret / 100 - beta * (base_line_ret - national_bond_ret / 100)
    return round(alpha, 4)


def sharp_ratio(ret: pd.Series, national_bond_ret: float):
    """
    夏普比率
    :param ret:
    :param national_bond_ret:
    :return:
    """
    var = volatility(ret)
    ret = annualized_ret(ret)
    ratio = (ret - national_bond_ret / 100) / var
    return round(ratio, 4)


def sortino_ratio(ret: pd.Series, national_bond_ret: float):
    """
    索提诺比率
    :param ret:
    :param national_bond_ret:
    :return:
    """
    downside_vol = downside_volatility(ret)
    ret = annualized_ret(ret)
    ratio = (ret - national_bond_ret / 100) / downside_vol
    return round(ratio, 4)


if __name__ == '__main__':
    import akshare as ak

    bond_china_yield_df = ak.bond_china_yield(start_date="20250201", end_date="20250501")
    print(bond_china_yield_df)













