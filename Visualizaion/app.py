# app.py 完整代码
from flask import Flask, render_template
from tools import create_chart
from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB

app = Flask(__name__)


@app.route('/')
def index():
    mongo = MongoDB('BackTestResults')
    strategies = mongo.show_collections()
    results = mongo.get_test_result(strategies[0], test_date=test_date)
    return render_template('index.html', charts=[create_chart(df, buy_idx=[36], sell_idx=[])])


if __name__ == '__main__':
    app.run(debug=True)