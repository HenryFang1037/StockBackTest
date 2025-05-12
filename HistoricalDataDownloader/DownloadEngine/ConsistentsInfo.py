import warnings
from datetime import datetime
from functools import lru_cache

from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB
from HistoricalDataDownloader.ConceptHistoricalData.ConceptList import get_all_concepts_list
from HistoricalDataDownloader.IndexHistoricalData.IndexList import index_symbols
from HistoricalDataDownloader.StockHistoricalData.StockList import get_all_stocks_list

warnings.filterwarnings("ignore")


def now():
    return f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} <--> "


@lru_cache()
def get_all_consistent():
    mongo = MongoDB(database_name='沪深A股成分组成')
    print(f"{now()}开始下载沪深A股组成信息")
    print(f"{now()}1. 开始下载沪深A股股票信息")
    stock_info = get_all_stocks_list()
    mongo.update_insert(table_name='沪深A股票信息', dict_data=stock_info.to_dict('records'),
                        filter_key=stock_info.columns[0])
    print(f"{now()}1. 完成下载沪深A股股票信息")
    print(f"{now()}2. 开始下载沪深A股概念信息")
    concept_info = get_all_concepts_list()
    mongo.update_insert(table_name='沪深A股概念信息', dict_data=concept_info.to_dict('records'),
                        filter_key=concept_info.columns[0])
    print(f"{now()}2. 完成下载沪深A股概念信息")
    return stock_info, concept_info, index_symbols


if __name__ == '__main__':
    results = get_all_consistent()
