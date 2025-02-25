import uuid
import warnings
from datetime import datetime
import aiohttp
import pandas as pd
import asyncio
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame

from DatabaseTools.AsyncMysqlTools import get_mysql

warnings.filterwarnings('ignore')


def code_id_map_em() -> dict:
    """
    东方财富-股票和市场代码
    http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 股票和市场代码
    :rtype: dict
    """
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:1 t:2,m:1 t:23",
        "fields": "f12",
        "_": "1623833739532",
    }

    r = requests.get(url, params=params)
    data_json =  r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df["market_id"] = 1
    temp_df.columns = ["sh_code", "sh_id"]
    code_id_dict = dict(zip(temp_df["sh_code"], temp_df["sh_id"]))
    params = {
        "pn": "1",
        "pz": "5000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80",
        "fields": "f12",
        "_": "1623833739532",
    }

    r = requests.get(url, params=params)
    data_json =  r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["sz_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["sz_id"])))
    params = {
        "pn": "1",
        "pz": "5000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:81 s:2048",
        "fields": "f12",
        "_": "1623833739532",
    }

    r = requests.get(url, params=params)
    data_json =  r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["bj_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["bj_id"])))
    return code_id_dict


async def stock_individual_info(symbol: str = "603777", code_id_dict: dict = {}) -> pd.DataFrame:
    """
    东方财富-个股-股票信息
    http://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :return: 股票信息
    :rtype: pandas.DataFrame
    """

    url = "http://push2.eastmoney.com/api/qt/stock/get"
    params = {
        'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
        'fltt': '2',
        'invt': '2',
        'fields': 'f120,f121,f122,f174,f175,f59,f163,f43,f57,f58,f169,f170,f46,f44,f51,f168,f47,f164,f116,f60,f45,f52,f50,f48,f167,f117,f71,f161,f49,f530,f135,f136,f137,f138,f139,f141,f142,f144,f145,f147,f148,f140,f143,f146,f149,f55,f62,f162,f92,f173,f104,f105,f84,f85,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f107,f111,f86,f177,f78,f110,f262,f263,f264,f267,f268,f255,f256,f257,f258,f127,f199,f128,f198,f259,f260,f261,f171,f277,f278,f279,f288,f152,f250,f251,f252,f253,f254,f269,f270,f271,f272,f273,f274,f275,f276,f265,f266,f289,f290,f286,f285,f292,f293,f294,f295',
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        '_': '1640157544804',
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_json = await r.json()
    temp_df = pd.DataFrame(data_json)
    temp_df.reset_index(inplace=True)
    del temp_df['rc']
    del temp_df['rt']
    del temp_df['svr']
    del temp_df['lt']
    del temp_df['full']
    code_name_map = {
        'f57': '股票代码',
        'f58': '股票简称',
        'f84': '总股本',
        'f85': '流通股',
        'f127': '行业',
        'f116': '总市值',
        'f117': '流通市值',
        'f189': '上市时间',
    }
    temp_df['index'] = temp_df['index'].map(code_name_map)
    temp_df = temp_df[pd.notna(temp_df['index'])]
    if 'dlmkts' in temp_df.columns:
        del temp_df['dlmkts']
    temp_df.columns = [
        'item',
        'value',
    ]
    temp_df.reset_index(inplace=True, drop=True)
    print(temp_df)
    return temp_df


async def all_board_stats() -> pd.DataFrame:
    """
    东方财富网-沪深板块-行业板块-名称
    http://quote.eastmoney.com/center/boardlist.html#industry_board
    :return: 行业板块-名称
    :rtype: pandas.DataFrame
    """
    mysql = await get_mysql(database='board_daily_history')

    async def save_to_mysql(code, data):
        await mysql.create_table(code)
        data.set_index('uuid', inplace=True)
        res = [tuple(d) for d in data.to_records()]
        # 巨坑， numpy.record数据 在aiomysql解析中会解析成字符串，导致执行sql语句时数据匹配错误
        await mysql.update_insert(code, res)

    url = "http://17.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "2000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:90 t:2 f:!50",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222",
        "_": "1626075887768",
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_json = await r.json()
        temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_df.reset_index(inplace=True)
        temp_df["index"] = temp_df.index + 1
        temp_df.columns = [
            "排名",
            "-",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "-",
            "_",
            "-",
            "换手率",
            "-",
            "-",
            "-",
            "板块代码",
            "-",
            "板块名称",
            "-",
            "-",
            "-",
            "-",
            "总市值",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "上涨家数",
            "下跌家数",
            "-",
            "-",
            "-",
            "领涨股票",
            "-",
            "-",
            "领涨股票-涨跌幅",
            "-",
            "-",
            "-",
            "-",
            "-",
        ]
        date = datetime.now().strftime('%Y-%m-%d')
        temp_df['uuid'] = temp_df["板块代码"].apply(lambda x: uuid.uuid5(uuid.NAMESPACE_URL, x + date))
        temp_df['日期'] = date
        temp_df = temp_df[
            [
                "uuid",
                "日期",
                "排名",
                "板块名称",
                "板块代码",
                "最新价",
                "涨跌额",
                "涨跌幅",
                "总市值",
                "换手率",
                "上涨家数",
                "下跌家数",
            ]
        ]
        temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="raise")
        temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="raise")
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], downcast="float")
        temp_df["上涨家数"] = pd.to_numeric(temp_df["上涨家数"])
        temp_df["下跌家数"] = pd.to_numeric(temp_df["下跌家数"])

        await save_to_mysql('board_stats', temp_df)
        end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("{}<<-->>{}个行业板块统计数据已完成下载.".format(end, temp_df.shape[0]))
        return temp_df


async def stock_zh_a_hist(symbol: str = "000001", period: str = "daily", start_date: str = "19700101",
                          end_date: str = "20500101", adjust: str = "qfq", code_id_dict: dict = {}, ) -> DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    http://quote.eastmoney.com/concept/sh603777.html?from=classic
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
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {"fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
              "ut": "7eea3edcaed734bea9cbfc24409ed989", "klt": period_dict[period], "fqt": adjust_dict[adjust],
              "secid": f"{code_id_dict[symbol]}.{symbol}", "beg": start_date, "end": end_date, "_": "1623766962675", }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_json = await r.json()
        if not (data_json["data"] and data_json["data"]["klines"]):
            return pd.DataFrame()
        temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
        temp_df.columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额",
                           "换手率"]
        temp_df.index = pd.to_datetime(temp_df["日期"])
        temp_df.reset_index(inplace=True, drop=True)

        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
        temp_df["最高"] = pd.to_numeric(temp_df["最高"])
        temp_df["最低"] = pd.to_numeric(temp_df["最低"])
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], downcast="float")
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], downcast="float")

    return temp_df


async def download_stock_daily_history(code, start_date, end_date, code_id_dict, period='daily', adjust='qfq'):
    """
    下载沪深A股个股日度历史数据
    :param code: 股票代码
    :param start_date: 下载开始日期
    :param end_date: 下载截止日期
    :param code_id_dict: 东方财富Api股票id字典
    :param period:历史数据维度，默认daily
    :param adjust: 赋权方式，默认前赋权
    :return:
    """
    try:
        res = await stock_zh_a_hist(code, start_date=start_date, end_date=end_date, code_id_dict=code_id_dict,
                                    period=period, adjust=adjust)
        return code, res
    except Exception as e:
        return code, None


async def download_all_stocks_daily_history(codes, code_id_dict, start_date, end_date):
    """
    下载沪深A股所有股票日度数据
    :param codes: 沪深A股所有股票代码
    :param code_id_dict: 东方财富Api股票id字典
    :param start_date: 下载开始日期， 格式%Y%m%d
    :param end_date: 下载截止日期， 格式%Y%m%d
    :return: 保存到Mysql数据库
    """
    num = len(codes)
    mysql = await get_mysql(database='stock_daily_history')
    print("{}<<-->>开始下载{}沪深A股日度数据.".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), num))

    async def save_to_mysql(code, data):
        await mysql.create_table(code)
        # 股票停牌时，会返回空的Pandas.DataFrame
        if data.empty:
            return
        data.set_index('日期', inplace=True)
        res = [tuple(d) for d in data.to_records()]

        # 巨坑， numpy.record数据 在aiomysql解析中会解析成字符串，导致执行sql语句时数据匹配错误
        await mysql.update_insert(code, res)

    async def download(codes, code_id_dict=code_id_dict):
        retry_codes = []
        task_list = []
        for code in codes:
            task = asyncio.create_task(
                download_stock_daily_history(code, start_date=start_date, end_date=end_date, code_id_dict=code_id_dict))
            task_list.append(task)

        for task in asyncio.as_completed(task_list):
            code, data = await task
            if data is None:
                retry_codes.append(code)
            else:
                await save_to_mysql(code, data)

        return retry_codes

    while len(codes):
        codes = await download(codes, code_id_dict=code_id_dict)

    mysql.close()
    end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("{}<<-->>{}沪深A股已完成下载.".format(end, num))