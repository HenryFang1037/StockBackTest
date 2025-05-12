# app.py 完整代码
from flask import Flask, render_template
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objs as go
from plotly.subplots import make_subplots

app = Flask(__name__)


# def create_chart():
#     from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB
#     df = MongoDB('沪深A股日度数据').find('300251', start_date='20240501', end_date='20250506')
#     df['日期'] = pd.to_datetime(df['日期'])
#     date_all = pd.date_range(df['日期'].min(), df['日期'].max())
#     date_remove = [d for d in date_all.tolist() if not d in df['日期'].tolist()]
#     # 创建带子图的画布
#     fig = make_subplots(
#         rows=2, cols=1,
#         shared_xaxes=True,
#         vertical_spacing=0.1,
#         row_heights=[0.75, 0.25],  # K线图占70%，成交量占30%
#         specs=[[{"secondary_y": True}], [{}]]
#     )
#
#     # 添加K线图（第一行）
#     fig.add_trace(
#         go.Candlestick(
#             x=df.日期.apply(lambda x: x.strftime('%Y-%m-%d')),
#             open=df.开盘,
#             high=df.最高,
#             low=df.最低,
#             close=df.收盘,
#             name='K线',
#             increasing_line_color='#E74C3C',  # 上涨颜色
#             decreasing_line_color='#2ECC71'  # 下跌颜色
#         ),
#         row=1, col=1
#     )
#
#     # 添加成交量柱状图（第二行）
#     colors = ['#E74C3C' if 收盘 > 开盘 else '#2ECC71'
#               for 收盘, 开盘 in zip(df.收盘, df.开盘)]
#
#     fig.add_trace(
#         go.Bar(
#             x = df.日期.apply(lambda x: x.strftime('%Y-%m-%d')),
#             y=df.成交量,
#             name='成交量',
#             marker_color=colors,
#             opacity=0.7
#         ),
#         row=2, col=1
#     )
#
#     # 添加买卖信号标记（示例）
#     buy_dates = df.iloc[[5, 10, 15]].日期
#     sell_dates = df.iloc[[20, 25]].日期
#
#     fig.add_trace(
#         go.Scatter(
#             x=buy_dates,
#             y=df.loc[df.日期.isin(buy_dates), '收盘'],
#             mode='markers',
#             name='买入',
#             marker=dict(
#                 symbol='triangle-up',
#                 color='#00FF00',
#                 size=12,
#                 line=dict(width=1, color='black')
#             )),
#             row=1, col=1
#        )
#
#     fig.add_trace(
#         go.Scatter(
#             x=sell_dates,
#             y=df.loc[df.日期.isin(sell_dates), '收盘'],
#             mode='markers',
#             name='卖出',
#             marker=dict(
#                 symbol='triangle-down',
#                 color='#FF0000',
#                 size=12,
#                 line=dict(width=1, color='black')
#             ) ),
#             row=1, col=1
#        )
#
#     # 布局配置
#     fig.update_layout(
#         title='带成交量的股票K线图',
#         template='plotly_dark',
#         # hovermode='x unified',
#         showlegend=True,
#         margin=dict(t=40, b=20),
#         xaxis2=dict(
#             title='日期',
#             type='category',
#             rangeslider=dict(visible=False),  # 只在主图显示范围滑块
#
#         ),
#         yaxis=dict(title='价格'),
#         yaxis2=dict(title='成交量'),
#         dragmode='pan',  # 默认拖动模式
#         xaxis=dict(
#             rangeselector=dict(
#                 buttons=list([
#                     dict(count=1, label="1D", step="day", stepmode="backward"),
#                     dict(count=5, label="5D", step="day", stepmode="backward"),
#                     dict(step="all")
#                 ])
#             ),
#             type='category'
#             # rangeslider=dict(visible=True, thickness=0.08,bgcolor='rgba(0,0,0,0.1)')
#         ),
#         # xaxis_rangeslider_visible=False
#     )
#
#     # 隐藏成交量图的重复日期标签
#     fig.update_xaxes(showticklabels=False, row=2, col=1, tickformat='%Y-%m-%d')
#
#     return fig.to_html(full_html=False)

from Dataformat import create_chart

from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB
df = MongoDB('沪深A股日度数据').find('300251', start_date='20240501', end_date='20250506')

@app.route('/')
def index():
    return render_template('index.html', chart_html=create_chart(df, signals={'buy_idx': [30,]}))


if __name__ == '__main__':
    app.run(debug=True)