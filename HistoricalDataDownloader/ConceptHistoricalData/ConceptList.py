from datetime import datetime
from functools import lru_cache

import pandas as pd
import requests


@lru_cache()
def stock_board_concept_name_em() -> pd.DataFrame:
    """
    东方财富网-行情中心-沪深京板块-概念板块-名称
    https://quote.eastmoney.com/center/boardlist.html#concept_board
    :return: 概念板块-名称
    :rtype: pandas.DataFrame
    """
    url = "https://79.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "2",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:90 t:3 f:!50",
        "fields": "f2,f3,f4,f8,f12,f14,f15,f16,f17,f18,f20,f21,f24,f25,f22,f33,f11,f62,f128,f124,f107,f104,f105,f136",
        "_": "1626075887768",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["data"]["diff"]).T
    temp_df.reset_index(inplace=True)
    temp_df["index"] = range(1, len(temp_df) + 1)
    temp_df.columns = [
        "排名",
        "最新价",
        "涨跌幅",
        "涨跌额",
        "换手率",
        "_",
        "板块代码",
        "板块名称",
        "_",
        "_",
        "_",
        "_",
        "总市值",
        "_",
        "_",
        "_",
        "_",
        "_",
        "_",
        "上涨家数",
        "下跌家数",
        "_",
        "_",
        "领涨股票",
        "_",
        "_",
        "领涨股票-涨跌幅",
    ]
    temp_df = temp_df[
        [
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
            "领涨股票",
            "领涨股票-涨跌幅",
        ]
    ]
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df["上涨家数"] = pd.to_numeric(temp_df["上涨家数"], errors="coerce")
    temp_df["下跌家数"] = pd.to_numeric(temp_df["下跌家数"], errors="coerce")
    temp_df["领涨股票-涨跌幅"] = pd.to_numeric(
        temp_df["领涨股票-涨跌幅"], errors="coerce"
    )
    return temp_df


def get_all_concepts_list():
    result = stock_board_concept_name_em()
    result = result.rename(columns={'板块代码': '概念板块代码', '板块名称': '概念板块名称'})
    result['更新日期'] = datetime.now().strftime('%Y%m%d')
    return result[['概念板块代码', '概念板块名称', '更新日期']]


@lru_cache()
def get_all_concepts_pct_rank():
    result = stock_board_concept_name_em()
    result = result.rename(columns={'板块代码': '概念板块代码', '板块名称': '概念板块名称'})
    result['日期'] = datetime.now().strftime('%Y%m%d')
    result = result[['日期', '概念板块代码', '概念板块名称', '涨跌幅', '换手率', '上涨家数', '下跌家数']]
    result['日期'] = result['日期'].astype(str)
    result = result.sort_values(by='涨跌幅', ascending=False)
    return result


if __name__ == '__main__':
    print(get_all_concepts_list().to_dict())
