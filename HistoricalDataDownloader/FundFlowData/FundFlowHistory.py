import json
import time

import aiohttp
import pandas as pd


async def get_stock_fund_flow(symbol) -> (str, pd.DataFrame):
    """
        东方财富网-数据中心-资金流向-个股
        https://data.eastmoney.com/zjlx/detail.html
        :param symbol: 股票代码
        :type symbol: str
        :return: 近期个股的资金流数据
        :rtype: str, pandas.DataFrame
        """
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    }
    market = 0 if symbol.startswith("30") else 1
    params = {
        "lmt": "0",
        "klt": "101",
        "secid": f"{market}.{symbol}",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "_": int(time.time() * 1000),
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params, headers=headers)
        json_data = await r.json()
    content_list = json_data["data"]["klines"]
    temp_df = pd.DataFrame([item.split(",") for item in content_list])
    if temp_df.empty:
        return symbol, pd.DataFrame()
    temp_df.columns = [
        "日期",
        "主力净流入-净额",
        "小单净流入-净额",
        "中单净流入-净额",
        "大单净流入-净额",
        "超大单净流入-净额",
        "主力净流入-净占比",
        "小单净流入-净占比",
        "中单净流入-净占比",
        "大单净流入-净占比",
        "超大单净流入-净占比",
        "收盘价",
        "涨跌幅",
        "-",
        "-",
    ]
    temp_df = temp_df[
        [
            "日期",
            "收盘价",
            "涨跌幅",
            "主力净流入-净额",
            "主力净流入-净占比",
            "超大单净流入-净额",
            "超大单净流入-净占比",
            "大单净流入-净额",
            "大单净流入-净占比",
            "中单净流入-净额",
            "中单净流入-净占比",
            "小单净流入-净额",
            "小单净流入-净占比",
        ]
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").dt.date
    temp_df["主力净流入-净额"] = pd.to_numeric(
        temp_df["主力净流入-净额"], errors="coerce"
    )
    temp_df["小单净流入-净额"] = pd.to_numeric(
        temp_df["小单净流入-净额"], errors="coerce"
    )
    temp_df["中单净流入-净额"] = pd.to_numeric(
        temp_df["中单净流入-净额"], errors="coerce"
    )
    temp_df["大单净流入-净额"] = pd.to_numeric(
        temp_df["大单净流入-净额"], errors="coerce"
    )
    temp_df["超大单净流入-净额"] = pd.to_numeric(
        temp_df["超大单净流入-净额"], errors="coerce"
    )
    temp_df["主力净流入-净占比"] = pd.to_numeric(
        temp_df["主力净流入-净占比"], errors="coerce"
    )
    temp_df["小单净流入-净占比"] = pd.to_numeric(
        temp_df["小单净流入-净占比"], errors="coerce"
    )
    temp_df["中单净流入-净占比"] = pd.to_numeric(
        temp_df["中单净流入-净占比"], errors="coerce"
    )
    temp_df["大单净流入-净占比"] = pd.to_numeric(
        temp_df["大单净流入-净占比"], errors="coerce"
    )
    temp_df["超大单净流入-净占比"] = pd.to_numeric(
        temp_df["超大单净流入-净占比"], errors="coerce"
    )
    temp_df["收盘价"] = pd.to_numeric(temp_df["收盘价"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    return symbol, temp_df


async def get_concept_fund_flow(symbol) -> (str, pd.DataFrame):
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    params = {
        "lmt": "0",
        "klt": "101",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "secid": f"90.{symbol}",
        "_": "1678954135116",
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_json = await r.json()
        temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    if temp_df.empty:
        return symbol, pd.DataFrame()
    temp_df.columns = [
        "日期",
        "主力净流入-净额",
        "小单净流入-净额",
        "中单净流入-净额",
        "大单净流入-净额",
        "超大单净流入-净额",
        "主力净流入-净占比",
        "小单净流入-净占比",
        "中单净流入-净占比",
        "大单净流入-净占比",
        "超大单净流入-净占比",
        "-",
        "-",
        "-",
        "-",
    ]
    temp_df = temp_df[
        [
            "日期",
            "主力净流入-净额",
            "主力净流入-净占比",
            "超大单净流入-净额",
            "超大单净流入-净占比",
            "大单净流入-净额",
            "大单净流入-净占比",
            "中单净流入-净额",
            "中单净流入-净占比",
            "小单净流入-净额",
            "小单净流入-净占比",
        ]
    ]
    temp_df["主力净流入-净额"] = pd.to_numeric(
        temp_df["主力净流入-净额"], errors="coerce"
    )
    temp_df["主力净流入-净占比"] = pd.to_numeric(
        temp_df["主力净流入-净占比"], errors="coerce"
    )
    temp_df["超大单净流入-净额"] = pd.to_numeric(
        temp_df["超大单净流入-净额"], errors="coerce"
    )
    temp_df["超大单净流入-净占比"] = pd.to_numeric(
        temp_df["超大单净流入-净占比"], errors="coerce"
    )
    temp_df["大单净流入-净额"] = pd.to_numeric(
        temp_df["大单净流入-净额"], errors="coerce"
    )
    temp_df["大单净流入-净占比"] = pd.to_numeric(
        temp_df["大单净流入-净占比"], errors="coerce"
    )
    temp_df["中单净流入-净额"] = pd.to_numeric(
        temp_df["中单净流入-净额"], errors="coerce"
    )
    temp_df["中单净流入-净占比"] = pd.to_numeric(
        temp_df["中单净流入-净占比"], errors="coerce"
    )
    temp_df["小单净流入-净额"] = pd.to_numeric(
        temp_df["小单净流入-净额"], errors="coerce"
    )
    temp_df["小单净流入-净占比"] = pd.to_numeric(
        temp_df["小单净流入-净占比"], errors="coerce"
    )
    temp_df["日期"] = pd.to_datetime(temp_df["日期"]).dt.date
    return symbol, temp_df


async def get_stocks_fund_flow_rank(indicator: str) -> (str, pd.DataFrame):
    """
       东方财富网-数据中心-资金流向-排名
       https://data.eastmoney.com/zjlx/detail.html
       :param indicator: choice of {"今日", "3日", "5日", "10日"}
       :type indicator: str
       :return: 指定 indicator 资金流向排行
       :rtype: pandas.DataFrame
       """
    indicator_map = {
        "今日": [
            "f62",
            "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124",
        ],
        "3日": [
            "f267",
            "f12,f14,f2,f127,f267,f268,f269,f270,f271,f272,f273,f274,f275,f276,f257,f258,f124",
        ],
        "5日": [
            "f164",
            "f12,f14,f2,f109,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f257,f258,f124",
        ],
        "10日": [
            "f174",
            "f12,f14,f2,f160,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f260,f261,f124",
        ],
    }
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": indicator_map[indicator][0],
        "po": "1",
        "pz": "50000",
        "pn": "1",
        "np": "2",
        "fltt": "2",
        "invt": "2",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "fs": "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",
        "fields": indicator_map[indicator][1],
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_json = await r.json()
    temp_df = pd.DataFrame(data_json["data"]["diff"]).T
    temp_df.reset_index(inplace=True)
    if temp_df.empty:
        return indicator, pd.DataFrame()
    temp_df["index"] = range(1, len(temp_df) + 1)
    if indicator == "今日":
        temp_df.columns = [
            "序号",
            "最新价",
            "今日涨跌幅",
            "代码",
            "名称",
            "今日主力净流入-净额",
            "今日超大单净流入-净额",
            "今日超大单净流入-净占比",
            "今日大单净流入-净额",
            "今日大单净流入-净占比",
            "今日中单净流入-净额",
            "今日中单净流入-净占比",
            "今日小单净流入-净额",
            "今日小单净流入-净占比",
            "_",
            "今日主力净流入-净占比",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "序号",
                "代码",
                "名称",
                "最新价",
                "今日涨跌幅",
                "今日主力净流入-净额",
                "今日主力净流入-净占比",
                "今日超大单净流入-净额",
                "今日超大单净流入-净占比",
                "今日大单净流入-净额",
                "今日大单净流入-净占比",
                "今日中单净流入-净额",
                "今日中单净流入-净占比",
                "今日小单净流入-净额",
                "今日小单净流入-净占比",
            ]
        ]
    elif indicator == "3日":
        temp_df.columns = [
            "序号",
            "最新价",
            "代码",
            "名称",
            "_",
            "3日涨跌幅",
            "_",
            "_",
            "_",
            "3日主力净流入-净额",
            "3日主力净流入-净占比",
            "3日超大单净流入-净额",
            "3日超大单净流入-净占比",
            "3日大单净流入-净额",
            "3日大单净流入-净占比",
            "3日中单净流入-净额",
            "3日中单净流入-净占比",
            "3日小单净流入-净额",
            "3日小单净流入-净占比",
        ]
        temp_df = temp_df[
            [
                "序号",
                "代码",
                "名称",
                "最新价",
                "3日涨跌幅",
                "3日主力净流入-净额",
                "3日主力净流入-净占比",
                "3日超大单净流入-净额",
                "3日超大单净流入-净占比",
                "3日大单净流入-净额",
                "3日大单净流入-净占比",
                "3日中单净流入-净额",
                "3日中单净流入-净占比",
                "3日小单净流入-净额",
                "3日小单净流入-净占比",
            ]
        ]
    elif indicator == "5日":
        temp_df.columns = [
            "序号",
            "最新价",
            "代码",
            "名称",
            "5日涨跌幅",
            "_",
            "5日主力净流入-净额",
            "5日主力净流入-净占比",
            "5日超大单净流入-净额",
            "5日超大单净流入-净占比",
            "5日大单净流入-净额",
            "5日大单净流入-净占比",
            "5日中单净流入-净额",
            "5日中单净流入-净占比",
            "5日小单净流入-净额",
            "5日小单净流入-净占比",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "序号",
                "代码",
                "名称",
                "最新价",
                "5日涨跌幅",
                "5日主力净流入-净额",
                "5日主力净流入-净占比",
                "5日超大单净流入-净额",
                "5日超大单净流入-净占比",
                "5日大单净流入-净额",
                "5日大单净流入-净占比",
                "5日中单净流入-净额",
                "5日中单净流入-净占比",
                "5日小单净流入-净额",
                "5日小单净流入-净占比",
            ]
        ]
    elif indicator == "10日":
        temp_df.columns = [
            "序号",
            "最新价",
            "代码",
            "名称",
            "_",
            "10日涨跌幅",
            "10日主力净流入-净额",
            "10日主力净流入-净占比",
            "10日超大单净流入-净额",
            "10日超大单净流入-净占比",
            "10日大单净流入-净额",
            "10日大单净流入-净占比",
            "10日中单净流入-净额",
            "10日中单净流入-净占比",
            "10日小单净流入-净额",
            "10日小单净流入-净占比",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "序号",
                "代码",
                "名称",
                "最新价",
                "10日涨跌幅",
                "10日主力净流入-净额",
                "10日主力净流入-净占比",
                "10日超大单净流入-净额",
                "10日超大单净流入-净占比",
                "10日大单净流入-净额",
                "10日大单净流入-净占比",
                "10日中单净流入-净额",
                "10日中单净流入-净占比",
                "10日小单净流入-净额",
                "10日小单净流入-净占比",
            ]
        ]
    return indicator, temp_df


async def get_market_fund_flow() -> pd.DataFrame():
    """
       东方财富网-数据中心-资金流向-大盘
       https://data.eastmoney.com/zjlx/dpzjlx.html
       :return: 近期大盘的资金流数据
       :rtype: pandas.DataFrame
       """
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    }
    params = {
        "lmt": "0",
        "klt": "101",
        "secid": "1.000001",
        "secid2": "0.399001",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "cb": "jQuery183003743205523325188_1589197499471",
        "_": int(time.time() * 1000),
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params, headers=headers)
        text_data = await r.text()
    json_data = json.loads(text_data[text_data.find("{"): -2])
    content_list = json_data["data"]["klines"]
    temp_df = pd.DataFrame([item.split(",") for item in content_list])
    if temp_df.empty:
        return '大盘资金流向', pd.DataFrame()
    temp_df.columns = [
        "日期",
        "主力净流入-净额",
        "小单净流入-净额",
        "中单净流入-净额",
        "大单净流入-净额",
        "超大单净流入-净额",
        "主力净流入-净占比",
        "小单净流入-净占比",
        "中单净流入-净占比",
        "大单净流入-净占比",
        "超大单净流入-净占比",
        "上证-收盘价",
        "上证-涨跌幅",
        "深证-收盘价",
        "深证-涨跌幅",
    ]
    temp_df = temp_df[
        [
            "日期",
            "上证-收盘价",
            "上证-涨跌幅",
            "深证-收盘价",
            "深证-涨跌幅",
            "主力净流入-净额",
            "主力净流入-净占比",
            "超大单净流入-净额",
            "超大单净流入-净占比",
            "大单净流入-净额",
            "大单净流入-净占比",
            "中单净流入-净额",
            "中单净流入-净占比",
            "小单净流入-净额",
            "小单净流入-净占比",
        ]
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").dt.date
    temp_df["上证-收盘价"] = pd.to_numeric(temp_df["上证-收盘价"], errors="coerce")
    temp_df["上证-涨跌幅"] = pd.to_numeric(temp_df["上证-涨跌幅"], errors="coerce")
    temp_df["深证-收盘价"] = pd.to_numeric(temp_df["深证-收盘价"], errors="coerce")
    temp_df["深证-涨跌幅"] = pd.to_numeric(temp_df["深证-涨跌幅"], errors="coerce")
    temp_df["主力净流入-净额"] = pd.to_numeric(
        temp_df["主力净流入-净额"], errors="coerce"
    )
    temp_df["主力净流入-净占比"] = pd.to_numeric(
        temp_df["主力净流入-净占比"], errors="coerce"
    )
    temp_df["超大单净流入-净额"] = pd.to_numeric(
        temp_df["超大单净流入-净额"], errors="coerce"
    )
    temp_df["超大单净流入-净占比"] = pd.to_numeric(
        temp_df["超大单净流入-净占比"], errors="coerce"
    )
    temp_df["大单净流入-净额"] = pd.to_numeric(
        temp_df["大单净流入-净额"], errors="coerce"
    )
    temp_df["大单净流入-净占比"] = pd.to_numeric(
        temp_df["大单净流入-净占比"], errors="coerce"
    )
    temp_df["中单净流入-净额"] = pd.to_numeric(
        temp_df["中单净流入-净额"], errors="coerce"
    )
    temp_df["中单净流入-净占比"] = pd.to_numeric(
        temp_df["中单净流入-净占比"], errors="coerce"
    )
    temp_df["小单净流入-净额"] = pd.to_numeric(
        temp_df["小单净流入-净额"], errors="coerce"
    )
    temp_df["小单净流入-净占比"] = pd.to_numeric(
        temp_df["小单净流入-净占比"], errors="coerce"
    )
    return '大盘资金流向', temp_df


if __name__ == '__main__':
    import asyncio

    asyncio.run(get_stocks_fund_flow_rank('今日'))
