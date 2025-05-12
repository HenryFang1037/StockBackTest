database_names = (
    'stock_daily_history', 'stock_financial_indicators', 'stocks_classification_sw'
)

database_table_schema_type = {
    'stock_financial_indicators': '(`record_date` DATE, `dividend` DOUBLE, `dividend_yield` DOUBLE, `eps` DOUBLE, '
                                  '`float_market_capital` DOUBLE, `float_shares` DOUBLE, `goodwill_in_net_assets` '
                                  'DOUBLE, `market_capital` DOUBLE, `navps` DOUBLE, `pb` DOUBLE, `pe_forecast` '
                                  'DOUBLE, `pe_lyr` DOUBLE, `pe_ttm` DOUBLE, `total_shares` DOUBLE))',
    'stock_daily_history': '(`date` DATE,`open` DOUBLE,`close` DOUBLE,`high` DOUBLE,`low` DOUBLE,`volume` DOUBLE,'
                           '`turnover` DOUBLE,`amplitude` DOUBLE,`ppct` DEC,`tpct` DEC, `tor` DOUBLE, PRIMARY KEY ('
                           '`date`))',
    'stocks_classification_sw': '(`stockCode` VARCHAR(20),  `stockName` VARCHAR(20),  `sw_1` VARCHAR(20), '
                                '`sw_2` VARCHAR(20), `sw_3` VARCHAR(20))'
}

database_table_schema = {
    'stock_financial_indicators': '(`record_date`, `dividend`, `dividend_yield`, `eps`, `float_market_capital`, '
                                  '`float_shares`, `goodwill_in_net_assets`, `market_capital`, `navps`, `pb`,'
                                  '`pe_forecast`, `pe_lyr`, `pe_ttm`, `total_shares`)',
    'stock_daily_history': '(`date`,`open`,`close`,`high`,`low`,`volume`,`turnover`,`amplitude`,`ppct`,`tpct`, `tor`)',
    'stocks_classification_sw': '(`stockCode`,  `stockName`,  `sw_1`, `sw_2`, `sw_3`)',
}

database_table_columns = {
    'stock_financial_indicators': ['record_date', 'dividend', 'dividend_yield', 'eps', 'float_market_capital',
                                   'float_shares', 'goodwill_in_net_assets', 'market_capital', 'navps', 'pb',
                                   'pe_forecast', 'pe_lyr', 'pe_ttm', 'total_shares'],
    'stock_daily_history': ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'amplitude', 'ppct', 'tpct',
                            'tor'],
    'stocks_classification_sw': ['stockCode', 'stockName', 'sw_1', 'sw_2', 'sw_3'],
}
