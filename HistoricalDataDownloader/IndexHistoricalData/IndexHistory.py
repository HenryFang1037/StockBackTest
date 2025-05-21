import akshare as ak
from datetime import datetime
from HistoricalDataDownloader.IndexHistoricalData.IndexList import index_symbols
from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB


def get_daily_index_history():
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} <--> 开始下载沪深指数数据")
    mongo = MongoDB('沪深指数日度数据')
    for symbol in index_symbols.keys():
        res = ak.stock_zh_index_daily(symbol=symbol)
        res['date'] = res['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        res.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'}, inplace=True)
        mongo.update_insert(symbol, dict_data=res.to_dict('records'), filter_key='日期')
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} <--> 完成下载沪深指数数据")


