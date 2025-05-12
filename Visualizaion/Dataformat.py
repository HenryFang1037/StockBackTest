import pandas as pd
from collections import defaultdict
from datetime import datetime
import plotly.graph_objs as go
from plotly.subplots import make_subplots


def create_chart(data: pd.DataFrame, signals: defaultdict(list)):
    data['日期'] = data['日期'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())
    all_dates = pd.date_range(data['日期'].min(), data['日期'].max()).tolist()
    dates = [datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d').date() for x in all_dates]
    remove_dates = [date for date in dates if date not in data['日期'].tolist()]
    print(remove_dates)

    colors = ['#E74C3C' if close > open else '#2ECC71' for close, open in zip(data['收盘'], data['开盘'])]

    # 创建带子图的画布
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.75, 0.25],  # K线图占70%，成交量占30%
        # row_width=[0.2, 0.7],
        subplot_titles=('', '成交量')
    )

    fig.add_trace(go.Candlestick(
        x=data['日期'],
        open=data['开盘'],
        high=data['收盘'],
        low=data['最低'],
        close=data['最高'],
        increasing_line_color='#E74C3C',
        decreasing_line_color='#2ECC71',
    ),
        row=1, col=1
    )

    fig.add_trace(go.Bar(
        x=data['日期'],
        y=data['成交量'],
        marker_color=colors,
        showlegend=False
    ),
        row=2, col=1
    )

    # 添加买卖信号标记（示例）
    buy_idx = signals.get('buy_idx', [])
    sell_idx = signals.get('sell_idx', [])
    if len(buy_idx) != 0:
        buy_dates = data.iloc[buy_idx].日期
        fig.add_trace(
            go.Scatter(
                x=buy_dates,
                y=data.loc[data.日期.isin(buy_dates), '收盘'],
                mode='markers',
                name='买入',
                marker=dict(
                    symbol='triangle-up',
                    color='green',
                    size=10,
                    line=dict(width=1, color='black')
                )),
            row=1, col=1
        )
    else:
        raise Exception(f'传入的买入信号为空值')
    if len(sell_idx) != 0:
        sell_dates = data.iloc[sell_idx].日期
        fig.add_trace(
            go.Scatter(
                x=sell_dates,
                y=data.loc[data.日期.isin(sell_dates), '收盘'],
                mode='markers',
                name='卖出',
                marker=dict(
                    symbol='triangle-down',
                    color='darkorange',
                    size=10,
                    line=dict(width=1, color='black')
                )),
            row=1, col=1
        )

        # 布局配置
        fig.update_layout(
                    title='带成交量的股票K线图',
                    template='plotly_dark',
                    # hovermode='x unified',
                    showlegend=True,
                    margin=dict(t=40, b=20),
                    xaxis2=dict(
                        title='日期',
                        type='category',
                        rangeslider=dict(visible=False),  # 只在主图显示范围滑块

                    ),
                    yaxis=dict(title='价格'),
                    yaxis2=dict(title='成交量'),
                    dragmode='pan',  # 默认拖动模式
                    xaxis=dict(
                        rangeselector=dict(
                            buttons=list([
                                dict(count=1, label="1D", step="day", stepmode="backward"),
                                dict(count=5, label="5D", step="day", stepmode="backward"),
                                dict(step="all")
                            ])
                        ),
                        type='category'
                        # rangeslider=dict(visible=True, thickness=0.08,bgcolor='rgba(0,0,0,0.1)')
                    ),
        )

        fig.update_xaxes(rangebreaks=[dict(values=remove_dates)])
        # return fig.to_html(full_html=False)
    return fig


if __name__ == '__main__':
    from DatabaseTools.MongoDBTools.MongoDBTools import MongoDB

    df = MongoDB('沪深A股日度数据').find('300251', start_date='20240501', end_date='20250506')
    fig = create_chart(df, signals={'buy_idx': [36]})
    fig.show()