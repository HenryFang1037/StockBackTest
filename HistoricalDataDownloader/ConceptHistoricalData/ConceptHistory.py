import asyncio

import aiohttp
import pandas as pd
from HistoricalDataDownloader.Semaphore import semaphore


async def get_concept_daily_history(
        symbol: str = "BK1129",
        start_date: str = "20220101",
        end_date: str = "20221128",
        period: str = "daily",
        adjust: str = "",
) -> (str, pd.DataFrame):
    """
    东方财富网-沪深板块-概念板块-历史行情
    https://quote.eastmoney.com/bk/90.BK0715.html
    :param symbol: 板块名称
    :type symbol: str
    :type period: 周期; choice of {"daily", "weekly", "monthly"}
    :param period: 板块名称
    :param start_date: 开始时间
    :type start_date: str
    :param end_date: 结束时间
    :type end_date: str
    :param adjust: choice of {'': 不复权, "qfq": 前复权, "hfq": 后复权}
    :type adjust: str
    :return: 历史行情
    :rtype: pandas.DataFrame
    """
    period_map = {
        "daily": "101",
        "weekly": "102",
        "monthly": "103",
    }
    # stock_board_concept_em_map = stock_board_concept_name_em()
    # stock_board_code = stock_board_concept_em_map[
    #     stock_board_concept_em_map["板块名称"] == symbol
    # ]["板块代码"].values[0]
    adjust_map = {"": "0", "qfq": "1", "hfq": "2"}
    url = "https://91.push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"90.{symbol}",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": period_map[period],
        "fqt": adjust_map[adjust],
        "beg": start_date,
        "end": end_date,
        "smplmt": "10000",
        "lmt": "1000000",
        "_": "1626079488673",
    }
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            r = await session.get(url, params=params)
            data_json = await r.json()
            temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
            if temp_df.empty:
                return symbol, pd.DataFrame()
            temp_df.columns = [
                "日期",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "振幅",
                "涨跌幅",
                "涨跌额",
                "换手率",
            ]
            temp_df = temp_df[
                [
                    "日期",
                    "开盘",
                    "收盘",
                    "最高",
                    "最低",
                    "涨跌幅",
                    "涨跌额",
                    "成交量",
                    "成交额",
                    "振幅",
                    "换手率",
                ]
            ]
            temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
            temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
            temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
            temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
            temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
            temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
            temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
            temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
            temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
            temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
            return symbol, temp_df


async def get_concept_current_minute_data(
        symbol: str = "BK1146", period: str = "1"
) -> (str, pd.DataFrame):
    """
    东方财富网-沪深板块-概念板块-分时历史行情
    https://quote.eastmoney.com/bk/90.BK0715.html
    :param symbol: 板块名称
    :type symbol: str
    :param period: choice of {"1", "5", "15", "30", "60"}
    :type period: str
    :return: 分时历史行情
    :rtype: pandas.DataFrame
    """
    # stock_board_concept_em_map = stock_board_concept_name_em()
    # print(stock_board_concept_em_map)
    # stock_board_code = stock_board_concept_em_map[
    #     stock_board_concept_em_map["板块名称"] == symbol
    # ]["板块代码"].values[0]
    if period == "1":
        url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "iscr": "0",
            "ndays": "1",
            "secid": f"90.{symbol}",
            "_": "1687852931312",
        }
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                r = await session.get(url, params=params)
                data_json = await r.json()
                temp_df = pd.DataFrame(
                    [item.split(",") for item in data_json["data"]["trends"]]
                )

                temp_df.columns = [
                    "日期时间",
                    "开盘",
                    "收盘",
                    "最高",
                    "最低",
                    "成交量",
                    "成交额",
                    "最新价",
                ]
                temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
                temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
                temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
                temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
                temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
                temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
                temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")

                # print(symbol, temp_df)
                return symbol, temp_df


if __name__ == '__main__':
    asyncio.run(get_concept_current_minute_data(symbol='BK1146'))
    # import akshare as ak
    # res = ak.stock_board_concept_hist_min_em()
