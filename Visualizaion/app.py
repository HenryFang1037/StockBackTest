# app.py 完整代码
from flask import Flask, render_template
from tools import create_chart

app = Flask(__name__)


from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB

df = MongoDB('沪深A股日度数据').find('300251', start_date='20240501', end_date='20250506')


@app.route('/')
def index():
    return render_template('index.html', charts=[create_chart(df, buy_idx=[36], sell_idx=[])])


if __name__ == '__main__':
    app.run(debug=True)