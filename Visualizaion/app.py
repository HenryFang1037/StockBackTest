# app.py 完整代码
from flask import Flask, render_template
from tools import create_chart

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html', charts=[create_chart(df, buy_idx=[36], sell_idx=[])])


if __name__ == '__main__':
    app.run(debug=True)