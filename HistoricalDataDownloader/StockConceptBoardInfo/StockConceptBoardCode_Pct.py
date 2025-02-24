import akshare as ak
import pymongo as mongo


def get_stock_concept_board_codes_pct():
    res = ak.stock_board_concept_name_em()
    concept_board_codes = res[['板块名称', '板块代码']]
    concept_board_pct = res[['涨跌幅', '上涨家数', '下跌家数']]
    return concept_board_codes, concept_board_pct