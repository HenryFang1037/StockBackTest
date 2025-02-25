import akshare as ak
import pymongo as mongo
from datetime import datetime


def get_concept_board_info():
    res = ak.stock_board_concept_name_em()
    concept_board_info = res[['板块名称', '板块代码', '涨跌幅', '上涨家数', '下跌家数']]
    concept_board_info['日期'] = datetime.now().strftime('%Y-%m-%d')
    concept_board_info = concept_board_info['日期', '板块名称', '板块代码', '涨跌幅', '上涨家数', '下跌家数']
    return concept_board_info


def get_concept_board_constitution(board_name, board_code):
    res = ak.stock_board_concept_cons_em(board_name)
    concept_board_constitution = res[['代码', '名称']]
    concept_board_constitution['概念板块名称'] = board_name
    concept_board_constitution['概念板块代码'] = board_code
    concept_board_constitution = concept_board_constitution.rename(columns={'代码': '股票代码', '名称': '股票名称'})
    return concept_board_constitution['概念板块名称', '概念板块代码', '股票名称', '股票代码']


if __name__ == '__main__':
    # Connect to MongoDB
    client = mongo.MongoClient('mongodb://localhost:27017/')
    db = client['stock_board_concept']

    # Get stock concept board codes and percentage
    # concept_board_info = get_stock_concept_board_codes_pct()
    res = get_concept_board_constitution('华为欧拉')
    print(res.columns)
    print(res)
