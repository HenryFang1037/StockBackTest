import asyncio

import aiohttp
import pandas as pd
from io import StringIO


async def get_bond_daily_history(symbol, start_date, end_date):
    """
        中国债券信息网-国债及其他债券收益率曲线
        https://www.chinabond.com.cn/
        https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/historyQuery?startDate=2019-02-07&endDate=2020-02-04&gjqx=0&qxId=ycqx&locale=cn_ZH
        注意: end_date - start_date 应该小于一年
        :param start_date: 需要查询的日期, 返回在该日期之后一年内的数据
        :type start_date: str
        :param end_date: 需要查询的日期, 返回在该日期之前一年内的数据
        :type end_date: str
        :return: 返回在指定日期之间之前一年内的数据
        :rtype: pandas.DataFrame
        """
    url = "https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/historyQuery"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/114.0.0.0 Safari/537.36"
    }
    params = {
        "startDate": "-".join([start_date[:4], start_date[4:6], start_date[6:]]),
        "endDate": "-".join([end_date[:4], end_date[4:6], end_date[6:]]),
        "gjqx": "0",
        "qxId": "ycqx",
        "locale": "cn_ZH",
    }
    async with aiohttp.ClientSession() as session:
        res = await session.get(url, params=params, headers=headers)
        data_text = await res.text()
        data_text = data_text.replace("&nbsp", "")
        data_df = pd.read_html(StringIO(data_text), header=0)[1]
        data_df["日期"] = pd.to_datetime(data_df["日期"], errors="coerce").dt.date
        data_df["3月"] = pd.to_numeric(data_df["3月"], errors="coerce")
        data_df["6月"] = pd.to_numeric(data_df["6月"], errors="coerce")
        data_df["1年"] = pd.to_numeric(data_df["1年"], errors="coerce")
        data_df["3年"] = pd.to_numeric(data_df["3年"], errors="coerce")
        data_df["5年"] = pd.to_numeric(data_df["5年"], errors="coerce")
        data_df["7年"] = pd.to_numeric(data_df["7年"], errors="coerce")
        data_df["10年"] = pd.to_numeric(data_df["10年"], errors="coerce")
        data_df["30年"] = pd.to_numeric(data_df["30年"], errors="coerce")
        data_df.sort_values(by="日期", inplace=True)
        data_df['日期'] = data_df['日期'].apply(lambda x: x.strftime('%Y-%m-%d'))
        data_df.reset_index(inplace=True, drop=True)
        # print(data_df)
        return symbol, data_df


if __name__ == '__main__':
    asyncio.run(get_bond_daily_history('bond', start_date='20240501', end_date='20250501'))
