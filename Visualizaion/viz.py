# app.py
from flask import Flask, render_template
import plotly
import plotly.graph_objs as go
import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta

app = Flask(__name__)


def generate_performance_curve():
    # 生成模拟收益曲线数据
    dates = pd.date_range(start='2023-01-01', periods=100)
    returns = np.cumsum(np.random.randn(100) * 0.01)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates,
        y=returns,
        mode='lines',
        name='策略收益'
    ))
    fig.update_layout(
        title='策略收益曲线',
        xaxis_title='日期',
        yaxis_title='收益率',
        showlegend=True
    )
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def generate_kline():
    # 生成模拟K线数据
    dates = pd.date_range(start='2023-01-01', periods=30)
    open = np.random.randn(30).cumsum() + 100
    high = open + np.abs(np.random.randn(30))
    low = open - np.abs(np.random.randn(30))
    close = open + np.random.randn(30)

    # 生成买卖点
    buy_dates = dates[::5]
    sell_dates = dates[3::5]

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=dates,
        open=open,
        high=high,
        low=low,
        close=close,
        name='K线'
    ))

    fig.add_trace(go.Scatter(
        x=buy_dates,
        y=[low[i] for i in range(0, 30, 5)],
        mode='markers',
        marker=dict(color='green', size=10),
        name='买入点'
    ))

    fig.add_trace(go.Scatter(
        x=sell_dates,
        y=[high[i] for i in range(3, 30, 5)],
        mode='markers',
        marker=dict(color='red', size=10),
        name='卖出点'
    ))

    fig.update_layout(
        title='K线图与买卖点',
        xaxis_title='日期',
        yaxis_title='价格',
        showlegend=True
    )
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route('/')
def index():
    # 生成风险指标数据
    risk_metrics = [
        {'name': '年化收益率', 'value': '15.2%'},
        {'name': '系统性风险', 'value': '0.85'},
        {'name': '非系统性风险', 'value': '0.45'},
        {'name': '收益波动率', 'value': '12.3%'},
        {'name': '夏普比率', 'value': '1.24'},
        {'name': '最大回撤', 'value': '-8.7%'}
    ]

    # 生成股票指标数据
    stock_metrics = [
        {'name': '最大收益', 'value': '23.5%'},
        {'name': '最小收益', 'value': '-5.2%'},
        {'name': '持有收益', 'value': '15.8%'},
        {'name': '持有天数', 'value': '63'}
    ]

    # 策略描述
    strategy_description = """这是一个基于均线突破的量化交易策略。策略规则：
    1. 当价格上穿20日均线时买入
    2. 当价格下穿20日均线时卖出
    3. 每次全仓操作
    4. 包含止盈止损机制"""

    return render_template('index_1.html',
                           performance_graph=generate_performance_curve(),
                           kline_graph=generate_kline(),
                           risk_metrics=risk_metrics,
                           stock_metrics=stock_metrics,
                           strategy_description=strategy_description)


if __name__ == '__main__':
    app.run(debug=True)