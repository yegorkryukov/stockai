import plotly.graph_objects as go

def plot_ticker_data(ticker_data, xaxis_rangeslider_visible=True):
    """
    Plot candelstick
    """
    ticker = ticker_data.ticker[0]
    
    fig = go.Figure(
        data = [
            go.Candlestick(
                x=ticker_data.index,
                open=ticker_data.open,
                high=ticker_data.high,
                low=ticker_data.low,
                close=ticker_data.adjclose,
                increasing_line_color= 'blue', 
                decreasing_line_color= 'red'
            )
        ],
        layout = go.Layout(
            title = go.layout.Title(text=f'{ticker}')
        )
    )

    # uncomment below to remove rangeslider
    fig.update_layout(xaxis_rangeslider_visible=xaxis_rangeslider_visible)

#     fig.show()
    return fig