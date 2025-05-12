import aiomysql
import pandas as pd

from MysqlSchema import database_names, database_table_schema, database_table_schema_type, database_table_columns


class AsyncMysql:
    def __init__(self, database):
        self.database = database
        self.pool = None

    async def init_pool(self):
        __pool = await aiomysql.create_pool(minsize=5, maxsize=100, host='127.0.0.1', port=3306, user='root',
                                            password='2008', db=self.database, autocommit=True)
        return __pool

    async def create_table(self, table):
        sql = f"CREATE TABLE IF NOT EXISTS `{table}` {database_table_schema_type[self.database]};"
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cor:
                await cor.execute(sql)

    async def drop_table(self, table):
        sql = f"DROP TABLE IF EXISTS `{table}`;"
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cor:
                await cor.execute(sql)

    async def update_insert(self, table: str, res: list) -> None:
        if self.database == 'stock_daily_history':
            sql = f"INSERT INTO `{table}` {database_table_schema[self.database]} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `open`=VALUES(`open`),`close`=VALUES(`close`),`high`=VALUES(`high`),`low`=VALUES(`low`),`volume`=VALUES(`volume`), `turnover`=VALUES(`turnover`),`amplitude`=VALUES(`amplitude`), `ppct`=VALUES(`ppct`),`tpct`=VALUES(`tpct`), `tor`=VALUES(`tor`);"
        elif self.database == 'stock_financial_indicators':
            sql = f"INSERT INTO `{table}` {database_table_schema[self.database]} VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `dividend`=VALUES(`dividend`), `dividend_yield`=VALUES(`dividend_yield`), `eps`=VALUES(`eps`), `pb`=VALUES(`pb`), `pe_forecast`=VALUES(`pe_forecast`), `pe_ttm`=VALUES(`pe_ttm`), `pe_lyr`=VALUES(`pe_lyr`);"
        else:
            raise Exception('没有指定数据库!!')

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, res)

    async def select(self, table, start_date=None, end_date=None, stockCode=None):
        if self.database not in database_names:
            raise Exception('没有指定数据库!!')
        columns = database_table_columns[self.database]
        if self.database == 'stock_daily_history':
            sql = f"SELECT * FROM `{table}` WHERE `date`>=date_format('{start_date}','%Y-%m-%d') AND `date`<=date_format('{end_date}','%Y-%m-%d')"
        elif self.database == 'stock_financial_indicators':
            sql = f"SELECT * FROM `{table}` WHERE `stockCode`={stockCode} AND `record_date`=date_format('{start_date}','%Y-%m-%d')"
        elif self.database == 'stocks_classification_sw':
            sql = f"SELECT * FROM `sw_industry_classification` WHERE `stockCode`={stockCode}"
        else:
            raise Exception(f'{table} is not a valid table')

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                r = await cur.fetchall()
                r = pd.DataFrame(r, columns=columns)
        return r

    async def close(self):
        await self.pool.close()


async def get_mysql(database):
    db = AsyncMysql(database=database)
    pool = await db.init_pool()
    db.pool = pool
    return db
