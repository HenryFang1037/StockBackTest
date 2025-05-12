import aiohttp
import pandas as pd
from akshare.utils import demjson


async def get_index_daily_history(symbol: str = "csi931151",
                                  start_date: str = "19900101",
                                  end_date: str = "20500101",
                                  ) -> (str, pd.DataFrame):
    """
    东方财富网-股票指数数据
    https://quote.eastmoney.com/center/hszs.html
    :param symbol: 带市场标识的指数代码; sz: 深交所, sh: 上交所, csi: 中信指数 + id(000905)
    :type symbol: str
    :param start_date: 开始时间
    :type start_date: str
    :param end_date: 结束时间
    :type end_date: str
    :return: 指数数据
    :rtype: pandas.DataFrame
    """
    market_map = {"sz": "0", "sh": "1", "csi": "2"}
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    if symbol.find("sz") != -1:
        secid = "{}.{}".format(market_map["sz"], symbol.replace("sz", ""))
    elif symbol.find("sh") != -1:
        secid = "{}.{}".format(market_map["sh"], symbol.replace("sh", ""))
    elif symbol.find("csi") != -1:
        secid = "{}.{}".format(market_map["csi"], symbol.replace("csi", ""))
    else:
        return symbol, pd.DataFrame()
    params = {
        "cb": "jQuery1124033485574041163946_1596700547000",
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101",  # 日频率
        "fqt": "0",
        "beg": start_date,
        "end": end_date,
        "_": "1596700547039",
    }

    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_text = await r.text()
        data_json = demjson.decode(data_text[data_text.find("{"): -2])
        temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
        # check temp_df data availability before further transformations which may raise errors
        if temp_df.empty:
            return symbol, pd.DataFrame()
        temp_df.columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "_"]
        temp_df = temp_df[["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额"]]
        temp_df['日期'] = temp_df['日期'].astype(str)
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
        temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
        temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
        return symbol, temp_df


if __name__ == '__main__':
    # asyncio.run(get_index_daily_history(symbol='sh000001'))
    # import akshare as ak
    # res = ak.stock_zh_index_daily_em(symbol="sh000001", start_date='20000101', end_date='20250506')
    # print(res)
    import requests

    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "cb": "jQuery1124033485574041163946_1596700547000",
        "secid": '1.000001',
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101",  # 日频率
        "fqt": "0",
        "beg": '19900101',
        "end": '20500101',
        "_": "1596700547039",
    }
    res = requests.get(url, params=params)
    print(res)
