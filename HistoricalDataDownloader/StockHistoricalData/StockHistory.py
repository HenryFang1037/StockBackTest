import aiohttp
import pandas as pd
from HistoricalDataDownloader.Semaphore import semaphore


async def get_stock_daily_history(
        symbol: str = '000001',
        start_date: str = "19700101",
        end_date: str = "20500101",
        period: str = "daily",
        adjust: str = "hfq",
        timeout: float = 15.,
) -> (str, pd.DataFrame):
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :param timeout: choice of None or a positive float number
    :type timeout: float
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    # code_id_dict = code_id_map_em()
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"1.{symbol}" if symbol.startswith("6") else f"0.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            r = await session.get(url, params=params, timeout=timeout)
            data_json = await r.json()
            if not (data_json["data"] and data_json["data"]["klines"]):
                return symbol, pd.DataFrame()
            temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
            # temp_df["证券代码"] = symbol
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
                "换手率"
            ]
            temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").astype(str)
            temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
            temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
            temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
            temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
            temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
            temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
            temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
            temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
            temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
            temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
            temp_df = temp_df[
                [
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
            ]
            return symbol, temp_df


async def get_stock_minute_history(
        symbol: str = "000001",
        start_date: str = "1979-09-01 09:32:00",
        end_date: str = "2222-01-01 09:32:00",
        adjust: str = "hfq",
) -> (str, pd.DataFrame):
    """
    东方财富网-行情首页-沪深京 A 股-每日分时行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param period: choice of {'1', '5', '15', '30', '60'}
    :type period: str
    :param adjust: choice of {'', 'qfq', 'hfq'}
    :type adjust: str
    :return: 每日分时行情
    :rtype: pandas.DataFrame
    """
    # code_id_dict = code_id_map_em()
    url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "ndays": "5",
        "iscr": "0",
        "secid": f"1.{symbol}" if symbol.startswith("6") else f"0.{symbol}",
        "_": "1623766962675",
    }
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            r = await session.get(url, params=params)
            data_json = await r.json()
            temp_df = pd.DataFrame(
                [item.split(",") for item in data_json["data"]["trends"]]
            )
            temp_df.columns = [
                "时间",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "均价",
            ]
            temp_df.index = pd.to_datetime(temp_df["时间"])
            temp_df = temp_df[start_date:end_date]
            temp_df.reset_index(drop=True, inplace=True)
            temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
            temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
            temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
            temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
            temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
            temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
            temp_df["均价"] = pd.to_numeric(temp_df["均价"], errors="coerce")
            temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
            return symbol, temp_df


if __name__ == "__main__":
    import asyncio

    asyncio.run(get_stock_daily_history(symbol='600000'))
