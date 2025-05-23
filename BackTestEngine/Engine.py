import json

import pandas as pd
from tqdm import tqdm
import os
import concurrent.futures
from datetime import datetime
from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB


class BaseEngine:
    def __init__(self, algorithm, start_date, end_date, data_type='stock', write_local=False):
        self.algorithm = algorithm
        self.start_date = start_date
        self.end_date = end_date
        self.type = data_type
        self.write_local = write_local

    def data(self):
        print(f'{datetime.now()}<-->开始加载数据')
        if self.type == 'stock':
            stocks = MongoDB(database_name='沪深A股成分组成').find(table_name='沪深A股票信息')
            stocks = stocks[['证券代码', '证券简称']]
            datas = []
            for i, row in tqdm(stocks.iterrows()):
                name, code = row['证券简称'], row['证券代码']
                df = MongoDB('沪深A股日度数据').find(code, start_date=self.start_date, end_date=self.end_date)
                if df.empty:
                    continue
                df['code'] = code
                df['name'] = name
                df = df.sort_values(by='日期', ascending=True)
                datas.append(df)
            print(f'{datetime.now()}<-->完成数据加载')
            return datas

        elif self.type == 'concept':
            concepts = MongoDB(database_name='沪深A股成分组成').find(table_name='沪深A股概念信息')
            concepts = concepts[['概念板块代码', '概念板块名称']]
            datas = []
            for i, row in tqdm(concepts.iterrows()):
                name, code = row['概念板块名称'], row['概念板块代码']
                df = MongoDB('概念板块日度数据').find(code, start_date=self.start_date, end_date=self.end_date)
                df['code'] = code
                df['name'] = name
                df = df.sort_values(by='日期', ascending=True)
                datas.append(df)
            print(f'{datetime.now()}<-->完成数据加载')
            return datas

        else:
            raise Exception(f'策略使用的数据类型错误:{self.type}')

    def run(self, args=(), kwargs={}):
        print(f'{datetime.now()}<-->开始运行{self.algorithm.__name__}策略')
        results = {}
        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
            future_symbol = {executor.submit(self.algorithm, data, *args, **kwargs): data['code'].unique()[0] for data in self.data()}
            for future in concurrent.futures.as_completed(future_symbol):
                symbol = future_symbol[future]
                try:
                    result = future.result()
                    results[symbol] = result
                except Exception as exc:
                    print(f'策略运行出错:{symbol}, {exc}')

        print(f'{datetime.now()}<-->{self.algorithm.__name__}策略运行完成')

        self.write_result(results)

    def write_result(self, results):
        print(f'{datetime.now()}<-->开始保存{self.algorithm.__name__}策略运行结果')
        pardir = os.path.abspath(os.path.pardir)
        results_dir = os.path.join(pardir, 'Results')
        algo_result_dir = os.path.join(results_dir, self.algorithm.__name__)
        if not os.path.exists(algo_result_dir):
            os.makedirs(algo_result_dir)
        if self.write_local is True:
            with open(f'{algo_result_dir}/{datetime.now().strftime("%Y-%m-%d")}.json', 'w') as f:
                # for key, res in results.items():
                #     if res is not None:
                #         f.write(res['comment'])
                #         f.newlines
                res = {k: val for k, val in results.items() if len(val)}
                json.dump(res, f)
            print(f'{datetime.now()}<-->{self.algorithm.__name__}策略运行结果已保存为json')
        else:
            mongo = MongoDB('BackTestResults')
            table_name = self.algorithm.__name__
            for key, res in results.items():
                if res is not None:
                    for val in res.values():
                        mongo.update_insert(table_name, val)
            print(f'{datetime.now()}<-->{self.algorithm.__name__}策略运行结果已保存在数据库中')


if __name__ == '__main__':
    from TradingStrategies.strategyA import gap_finder
    strategy_a = BaseEngine(algorithm=gap_finder, start_date='2024-11-01', end_date='2025-05-14', write_local=True)
    strategy_a.run()


