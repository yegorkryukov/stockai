import pandas as pd
import requests, os
from yahoo_fin import stock_info as si 
import numpy as np


def get_news_page(
    news_api_key: str = None,
    ticker: str = 'AAPL', 
    start_date: str = '2019-04-01', 
    end_date: str = '2020-10-15', 
    page: int = 1, 
    items: int = 50
    ) -> dict:
    """
    Obtains news per `ticker` from stocknewsapi.com
    """
    url = 'https://stocknewsapi.com/api/v1'
    
    start_date = pd.to_datetime(start_date).date().strftime('%m%d%Y')
    end_date   = pd.to_datetime(end_date).date().strftime('%m%d%Y')
    
    # docs for this api https://stocknewsapi.com/documentation
    params = {
        'tickers': ticker,
        'items'  : items,
        'token'  : news_api_key,
        'date'   : f'{start_date}-{end_date}',
        'country':'USA',
        'page'   : page,
        'type'   : 'article',
        'extra-fields' : 'id'
    }

    r = requests.get(url=url, params=params)
    results = r.json()
    pages = int(results['pages'])
    print(f'Got page {page} of {pages}')

    return {
        "data" : pd.DataFrame(results['data']),
        "pages" : pages
    }

def create_dataset(
    news_api_key: str = None,
    ticker: str = 'AAPL', 
    start_date: str = '2019-04-01', 
    end_date: str = '2020-10-15'
):
    """
    Creates a df with news + stock prices per ticker
    """
    # check if files exist
    filepath = f'./data/{ticker}_{start_date}_{end_date}.csv'
    print(filepath)
    
    if os.path.exists(filepath):
        print(f'File {filepath} exists. Reading and returning.')
        data = pd.read_csv(filepath)
    else:
        os.mkdir('./data/')
        result = get_news_page(
                    news_api_key=news_api_key,
                    ticker=ticker, 
                    start_date=start_date, 
                    end_date=end_date
                )
        data, pages = result['data'], result['pages']

        if pages > 1:
            page = 2
            while page <= pages:
                result = get_news_page(
                    news_api_key=news_api_key,
                    ticker=ticker, 
                    start_date=start_date, 
                    end_date=end_date, 
                    page=page
                )
                temp, pages = result['data'], result['pages']
                data = data.append(temp, ignore_index=True)
                page += 1
                data.to_csv(filepath, index=False)
    
    data.date = pd.to_datetime(data.date, errors='coerce', utc=True)
    t_data = si.get_data(ticker, start_date=start_date, end_date=end_date)

    data['all_text'] = data.title + data.text

    df = data.groupby(data.date.dt.date)['all_text'].apply(','.join).reset_index().set_index('date')
    df.columns = ['text']
    df = df.join(t_data)

    # fill adjclose for over the weekends and holiday
    # logic is the price on monday close is the result of 
    # the news over the weekend hence backfill
    df.adjclose = df.adjclose.fillna(method='backfill')
    df.close = df.close.fillna(method='backfill')

    # drop rows without the stock price
    df = df[df.adjclose.notna()]

    # drop rows without news
    df = df[df.text.notna()]

    # drop rows without date
    df = df[df.index.notna()]
    return df.drop(['open','high','low','close','volume','ticker'], axis=1)
