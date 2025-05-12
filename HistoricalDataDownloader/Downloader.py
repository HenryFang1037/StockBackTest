import asyncio
from datetime import datetime, timedelta
from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB
from HistoricalDataDownloader.ConceptHistoricalData.ConceptConsist import get_concept_cons_history
from HistoricalDataDownloader.ConceptHistoricalData.ConceptHistory import get_concept_daily_history
from HistoricalDataDownloader.IndexHistoricalData.IndexList import index_symbols
from HistoricalDataDownloader.StockHistoricalData.StockHistory import get_stock_daily_history
from HistoricalDataDownloader.DownloadEngine.ConsistentsInfo import get_all_consistent
from HistoricalDataDownloader.DownloadEngine.HistoricalData import StockDailyHistoricalDataDownload, ConceptDailyHistoricalDataDownload, ConceptConsistDailyDataDownload, IndexDailyHistoricalDataDownload


async def download_stock_daily_history():
    # 获取沪深A股股票代码
    stocks = MongoDB(database_name='沪深A股成分组成').find(table_name='沪深A股票信息')
    end_date = datetime.now().strftime('%Y%m%d')
    stocks['更新日期'] = stocks['更新日期'].apply(
        lambda x: datetime.strptime(x, '%Y%m%d') if type(x) is str else datetime(year=1900, month=1, day=1))

    # 排除下载退市股票数据
    dates = stocks['更新日期'].to_list()
    max_date, min_date = max(dates), min(dates)
    start_date = datetime.strftime(max_date-timedelta(days=3), '%Y%m%d')

    # max_date != min_date时min_date为退市股最后一天交易日期
    if max_date != min_date:
        symbols = stocks[stocks['更新日期'] == max_date]['证券代码'].to_list()
    else:
        symbols = stocks['证券代码'].to_list()
    if end_date == start_date:
        print('数据已是最新数据，无需再次下载')
        return
    stock_daily_history_downloader = StockDailyHistoricalDataDownload(
        download_api=get_stock_daily_history,
        save_database=MongoDB(database_name='沪深A股日度数据'))
    await stock_daily_history_downloader.run(symbols, start_date, end_date, filter_key='日期')


async def download_concept_daily_history():
    concepts = MongoDB(database_name='沪深A股成分组成').find(table_name='沪深A股概念信息')
    end_date = datetime.now().strftime('%Y%m%d')
    concepts['更新日期'] = concepts['更新日期'].apply(
        lambda x: datetime.strptime(x, '%Y%m%d') if type(x) is str else datetime(year=1900, month=1, day=1))

    # 排除下载退市股票数据
    dates = concepts['更新日期'].to_list()
    max_date, min_date = max(dates), min(dates)
    start_date = datetime.strftime(max_date-timedelta(days=3), '%Y%m%d')

    # max_date != min_date时min_date为退市股最后一天交易日期
    if max_date != min_date:
        symbols = concepts[concepts['更新日期'] == max_date]['概念板块代码'].to_list()
    else:
        symbols = concepts['概念板块代码'].to_list()

    if end_date == start_date:
        print('数据已是最新数据，无需再次下载')
        return
    concept_daily_history_downloader = ConceptDailyHistoricalDataDownload(
        download_api=get_concept_daily_history,
        save_database=MongoDB(database_name='概念板块日度数据'))
    await concept_daily_history_downloader.run(symbols, start_date, end_date, filter_key='日期')


async def download_concept_consist_daily_history():
    concepts = MongoDB(database_name='沪深A股成分组成').find(table_name='沪深A股概念信息')
    end_date = datetime.now().strftime('%Y%m%d')
    concepts['更新日期'] = concepts['更新日期'].apply(
        lambda x: datetime.strptime(x, '%Y%m%d') if type(x) is str else datetime(year=1900, month=1, day=1))

    # 排除下载退市股票数据
    dates = concepts['更新日期'].to_list()
    max_date, min_date = max(dates), min(dates)
    start_date = datetime.strftime(max_date-timedelta(days=30), '%Y%m%d')

    # max_date != min_date时min_date为退市股最后一天交易日期
    if max_date != min_date:
        symbols = concepts[concepts['更新日期'] == max_date]['概念板块代码'].to_list()
    else:
        symbols = concepts['概念板块代码'].to_list()

    if end_date == start_date:
        print('数据已是最新数据，无需再次下载')
        return

    concept_consist_history_downloader = ConceptConsistDailyDataDownload(
        download_api=get_concept_cons_history,
        save_database=MongoDB(database_name='概念板块成分股日度数据'))
    await concept_consist_history_downloader.run(symbols, start_date, end_date)


async def download_index_daily_history():
    symbols = index_symbols.keys()
    end_date = datetime.now().strftime('%Y%m%d')
    index_daily_history_downloader = IndexDailyHistoricalDataDownload(
        download_api=get_concept_daily_history,
        save_database=MongoDB(database_name='指数日度数据'))
    await index_daily_history_downloader.run(symbols, '20231001', end_date, filter_key='日期')


async def main_downloader():
    get_all_consistent()
    await download_stock_daily_history()
    await download_concept_daily_history()
    await download_concept_consist_daily_history()


if __name__ == '__main__':
    asyncio.run(main_downloader())
