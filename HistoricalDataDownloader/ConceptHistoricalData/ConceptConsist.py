from datetime import datetime

import aiohttp
import pandas as pd
from HistoricalDataDownloader.Semaphore import semaphore


# from ConceptList import stock_board_concept_name_em


async def get_concept_cons_history(symbol: str = "BK1146", start_date=None, end_date=None) -> (str, pd.DataFrame):
    """
    东方财富-沪深板块-概念板块-板块成份
    https://quote.eastmoney.com/center/boardlist.html#boards-BK06551
    :param symbol: 板块名称
    :type symbol: str
    :return: 板块成份
    :rtype: pandas.DataFrame
    """
    url = "https://29.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "2",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": f"b:{symbol} f:!50",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,"
                  "f24,f25,f22,f11,f62,f128,f136,f115,f152,f45",
        "_": "1626081702127",
    }
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            r = await session.get(url, params=params)
            data_json = await r.json()
            temp_df = pd.DataFrame(data_json["data"]["diff"]).T
            temp_df.reset_index(inplace=True)
            temp_df["index"] = range(1, len(temp_df) + 1)
            if temp_df.empty:
                return symbol, pd.DataFrame()
            temp_df.columns = [
                "序号",
                "_",
                "最新价",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "振幅",
                "换手率",
                "市盈率-动态",
                "_",
                "_",
                "代码",
                "_",
                "名称",
                "最高",
                "最低",
                "今开",
                "昨收",
                "_",
                "_",
                "_",
                "市净率",
                "_",
                "_",
                "_",
                "_",
                "_",
                "_",
                "_",
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
                    "涨跌幅",
                    "涨跌额",
                    "成交量",
                    "成交额",
                    "振幅",
                    "最高",
                    "最低",
                    "今开",
                    "昨收",
                    "换手率",
                    "市盈率-动态",
                    "市净率",
                ]
            ]
            temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
            temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
            temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
            temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
            temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
            temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
            temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
            temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
            temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce")
            temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce")
            temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
            temp_df["市盈率-动态"] = pd.to_numeric(temp_df["市盈率-动态"], errors="coerce")
            temp_df["市净率"] = pd.to_numeric(temp_df["市净率"], errors="coerce")
            temp_df['日期'] = datetime.now().strftime('%Y%m%d')

            return symbol, temp_df


if __name__ == '__main__':
    import asyncio

    asyncio.run(get_concept_cons_history(symbol='BK1146'))
