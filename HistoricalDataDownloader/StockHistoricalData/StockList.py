from datetime import datetime
from functools import lru_cache

import akshare as ak
import pandas as pd


@lru_cache()
def get_all_stocks_list():
    """
    Retrieves a comprehensive list of stocks from both Shanghai and Shenzhen stock exchanges.

    This function fetches stock information for A-shares listed on the Shanghai main board,
    Shanghai STAR Market (Science and Technology Innovation Board), and Shenzhen Stock Exchange.
    It combines and standardizes the data from these sources into a single DataFrame.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing the following columns:
        - stockCode (str): The unique identifier for each stock.
        - stockName (str): The name of the stock.
        - listDate (str): The date when the stock was first listed on the exchange.
        - board (str): The specific board or market where the stock is listed
          (e.g., '上海主板', '上海科创', or Shenzhen board types).

    Notes:
    ------
    This function relies on the akshare library to fetch stock data from Chinese markets.
    The resulting DataFrame includes stocks from multiple boards and exchanges, providing
    a comprehensive view of the Chinese A-share market.
    """
    shanghai_a_shares = ak.stock_info_sh_name_code(symbol='主板A股')
    shanghai_a_shares_tech = ak.stock_info_sh_name_code(symbol='科创板')
    shanghai_a_shares['板块'] = '上海主板'
    shanghai_a_shares_tech['板块'] = '上海科创'
    result = pd.concat([shanghai_a_shares, shanghai_a_shares_tech])
    result = result[['证券代码', '证券简称', '上市日期', '板块']]

    shenzhen_a_shares = ak.stock_info_sz_name_code(symbol='A股列表')
    shenzhen_a_shares = shenzhen_a_shares.rename(columns={'A股代码': '证券代码', 'A股简称': '证券简称',
                                                          'A股上市日期': '上市日期'})
    shenzhen_a_shares = shenzhen_a_shares[['证券代码', '证券简称', '上市日期', '板块']]
    result = pd.concat([result, shenzhen_a_shares])
    result['上市日期'] = result['上市日期'].astype(str)
    result['上市日期'] = result['上市日期'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%Y%m%d'))
    result['更新日期'] = datetime.now().strftime('%Y%m%d')
    return result


if __name__ == "__main__":
    from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB

    mongo = MongoDB(database_name='沪深A股成分组成')
    stock_info = get_all_stocks_list()
    mongo.update_insert(table_name='沪深A股票信息', dict_data=stock_info.to_dict('records'),
                        filter_key=stock_info.columns[0])
    # print(get_all_stocks_list().to_dict('records'))
