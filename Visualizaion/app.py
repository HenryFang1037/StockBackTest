# app.py 完整代码
from flask import Flask, render_template
from tools import create_chart
from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB
from datetime import datetime

app = Flask(__name__)


@app.route('/')
def index():
    mongo = MongoDB('BackTestResults')
    strategies = mongo.show_collections()
    results = list(mongo.get_test_result(strategies[0], test_date='2025-05-14'))
    mongo = MongoDB(database_name='沪深A股日度数据')
    df = mongo.find(results[0]['stock_code'], start_date=results[0]['start_date'], end_date=datetime.now().strftime('%Y-%m-%d'))
    return render_template('index.html', charts=[create_chart(df, buy_days=results[0]['buy_dates'], sell_days=results[0]['sell_dates'])])






if __name__ == '__main__':
    app.run(debug=True)
