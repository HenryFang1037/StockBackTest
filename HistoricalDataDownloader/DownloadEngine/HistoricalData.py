import asyncio
import types
from datetime import datetime


class DownloadError(Exception):
    def __init__(self):
        super(Exception, self).__init__()


class BaseHistoricalDataDownload():
    def __init__(self, download_api: types.MethodType, save_database: types.ModuleType):
        # self.download_data_name = download_data_name
        self._download_api = download_api
        self._save_database = save_database

    def now(self):
        return f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} <--> "

    async def __download(self, symbol, start_date, end_date):
        try:
            symbol, result = await self._download_api(symbol, start_date, end_date)
        except Exception as e:
            # print(f"Error downloading: {e}")
            return symbol, DownloadError()
        return symbol, result

    async def run(self, symbols: list, start_date=None, end_date=None, filter_key=None):
        async def _download_save(symbols, start_date, end_date, filter_key):
            tasks = []
            retry_symbols = []
            for symbol in symbols:
                task = asyncio.create_task(self.__download(symbol, start_date=start_date, end_date=end_date))
                tasks.append(task)
            for completed_task in asyncio.as_completed(tasks):
                symbol, result = await completed_task
                # print(symbol, result)
                if isinstance(result, DownloadError):
                    retry_symbols.append(symbol)
                elif not result.empty:
                    self._save_database.update_insert(table_name=symbol, dict_data=result.to_dict('records'),
                                                      filter_key=filter_key)
                else:
                    print(f"{symbol} failed to download, it's an empty dataframe")
            return retry_symbols

        print(f'{self.now()}开始下载{self._save_database.database_name}')
        while len(symbols):
            symbols = await _download_save(symbols, start_date=start_date, end_date=end_date, filter_key=filter_key)
        print(f'{self.now()}完成{self._save_database.database_name}下载')
        # self._save_database.close()


class StockDailyHistoricalDataDownload(BaseHistoricalDataDownload):
    def __init__(self, download_api, save_database):
        super(StockDailyHistoricalDataDownload, self).__init__(download_api, save_database)


class ConceptDailyHistoricalDataDownload(BaseHistoricalDataDownload):
    def __init__(self, download_api, save_database):
        super(ConceptDailyHistoricalDataDownload, self).__init__(download_api, save_database)


class ConceptConsistDailyDataDownload(BaseHistoricalDataDownload):
    def __init__(self, download_api, save_database):
        super(ConceptConsistDailyDataDownload, self).__init__(download_api, save_database)


class IndexDailyHistoricalDataDownload(BaseHistoricalDataDownload):
    def __init__(self, download_api, save_database):
        super(IndexDailyHistoricalDataDownload, self).__init__(download_api, save_database)


class StockFundFlowDataDownload(BaseHistoricalDataDownload):
    def __init__(self):
        super(StockFundFlowDataDownload, self).__init__()


class ConceptFundFlowDataDownload(BaseHistoricalDataDownload):
    def __init__(self):
        super(ConceptFundFlowDataDownload, self).__init__()


class MarketFundFlowDataDownload(BaseHistoricalDataDownload):
    def __init__(self):
        super(MarketFundFlowDataDownload, self).__init__()



