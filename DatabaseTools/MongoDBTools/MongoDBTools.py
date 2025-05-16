import pandas as pd
import pymongo


class MongoDB():
    def __init__(self, database_name):
        self.database_name = database_name
        self._client = pymongo.MongoClient("mongodb://localhost:27017/")
        self._db = self._client[self.database_name]

    def update_insert(self, table_name, dict_data, filter_key=None):
        if filter_key is not None:
            for data in dict_data:
                self._db[table_name].update_one({filter_key: data[filter_key]}, {'$set': data}, upsert=True)
        else:
            self._db[table_name].insert_many(dict_data)

    def find(self, table_name, start_date=None, end_date=None):
        if start_date is not None and end_date is not None:
            result = self._db[table_name].find({'日期': {'$gte': start_date, '$lte': end_date}}, {'_id': 0})
            result = pd.DataFrame(result)
        else:
            result = self._db[table_name].find({}, {'_id': 0})
            result = pd.DataFrame(result)
        return result

    def get_test_result(self, table_name, test_date=None):
        if test_date is not None:
            results = self._db[table_name].find({'test_date': test_date}, {'_id': 0})
        else:
            results = self._db[table_name].find({}, {'_id': 0})
        return pd.DataFrame(results)

    def show_collections(self):
        return self._db.list_collection_names()

    def drop_collection(self, table_name):
        return self._db.drop(table_name)
