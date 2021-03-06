def get_html_from_url(url):
    """Returns html content of the page at url 
    """
    import requests
    logger = create_logger()
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:79.0) Gecko/20100101 Firefox/79.0"}
    requests.adapters.DEFAULT_RETRIES = 1

    try:
        url_get = requests.get(url, headers=headers, timeout=3)
        if url_get.status_code == 200:
            return url_get.text
        else:
            logger.info(f'Cannot get html for {url}. Error: {url_get.status_code}')
            return False
    except Exception as e:
        logger.info(f'Cannot get html for {url}. Error: {e}')
        return False

def get_urls_finviz(ticker):
    """Obtain latest news urls for `ticker` from finviz.com

    Parameters
    ----------
    ticker :  str, ticker to collect news for

    Output
    ------
    urls   : list, list of urls
    """
    logger = create_logger()
    from bs4 import BeautifulSoup

    logger.info(f'Finviz.com: {ticker}: getting news urls')

    url = 'https://finviz.com/quote.ashx?t=' + str(ticker)

    result = get_html_from_url(url)

    if result:
        html = BeautifulSoup(result, 'html.parser')
        news_table = html.find(id='news-table')

        urls = [row.a['href'] for row in news_table.findAll('tr')]
        logger.info(f'Finviz.com: {ticker}: FOUND {len(urls)} URLs')
        return urls
    logger.info(f'Finviz.com: {ticker}: No URLs FOUND')
    return False

def get_urls_yahoo(ticker):
    """Obtain latest news urls for `ticker` from finance.yahoo.com

    Parameters
    ----------
    ticker :  str, ticker to collect news for

    Output
    ------
    urls   : list, list of urls
    """
    from selenium import webdriver
    import time
    from random import randint

    logger = create_logger()

    logger.info(f'Finance.yahoo.com: {ticker}: getting URLs')

    js = """var scrollingElement = (document.scrollingElement || document.body);
                    scrollingElement.scrollTop = scrollingElement.scrollHeight;"""

    url = f'https://finance.yahoo.com/quote/{ticker}'
    urls_list = set()
    
    try: 
        browser = webdriver.PhantomJS()
        browser.get(url)
        logger.info(f'Finance.yahoo.com: {ticker}: Headless Firefox Initialized for URL: {url}')
        if browser.current_url == f'https://finance.yahoo.com/lookup?s={ticker}':
            logger.info(f'Finance.yahoo.com: {ticker}: Extracting URLs failed. Ticker {ticker} does not exist')
            return False
        browser.execute_script(js)
        time.sleep(randint(3,10))
        items_list = browser.find_elements_by_xpath('//h3/a')
        num_of_items = 0
        while num_of_items != len(items_list):
            num_of_items = len(items_list)
            browser.execute_script(js)
            time.sleep(randint(3,5))
            items_list = browser.find_elements_by_xpath('//h3/a')
            logger.info(f'Finance.yahoo.com: {ticker}: found {len(items_list)} urls so far')

        for item in items_list:
            if not ('gemini' in item.get_attribute('href')):
                urls_list.add(item.get_attribute('href'))

        logger.info(f'Finance.yahoo.com: {ticker}: Found {len(urls_list)} urls')
    except Exception as e:
        logger.info(f'Finance.yahoo.com: {ticker}: Extracting URLs failed. Error:\n{e}')

    browser.quit()
    if urls_list:
        return list(urls_list)
    logger.info(f'Finance.yahoo.com: {ticker}: No URLs found')
    return False

def get_urls_reddit(start_date='', subreddits=[], stop_urls=[]):
    """Obtain latest news urls from reddit `subreddits`

    Parameters
    ----------
    start_date :  str, default '', format 'YYYY-MM-DD' date from to collect the news
                    if no start_date provided, getting the news for the last 24 hours
    subreddits: list, default [], if empty getting the news from `usanews` subreddit
    NOT IMPLEMENTED stop_urls: list of str, default [], domains to exclude from results, partial match

    Output
    ------
    urls   : list, list of urls
    """
    import datetime as dt
    from dateutil import parser
    from psaw import PushshiftAPI
    import pandas as pd
    logger = create_logger()

    api = PushshiftAPI()

    if start_date:
        start_date = int(parser.parse(start_date).timestamp())
    else:
        start_date = int((dt.datetime.today()-dt.timedelta(1)).timestamp())

    if not subreddits:
        subreddits = ['usanews']

    urls = set()
    for subreddit in subreddits:
        logger.info(f'Reddit: {subreddit}: Starting URLs collection from {dt.datetime.fromtimestamp(start_date)}')
        data = pd.DataFrame(
            api.search_submissions(
                after=start_date,
                subreddit=subreddit,
                filter=['url']))
        if len(data) > 0: urls |= set(data.url)
    
    if len(urls) > 0: 
        logger.info(f'Reddit: {subreddit}: Found {len(urls)} urls')
        return list(urls)
    
    logger.info('Reddit: {subreddit}: No urls found')
    return False

def save_urls_to_db(urls, ticker=''):
    """Saves urls to MongoDB checking for duplicates
    """
    import pymongo as pm
    import pandas as pd
    logger = create_logger()

    logger.info(f'SAVE TO DB: Got {len(urls)} URLs to save')

    client = pm.MongoClient('mongodb://localhost:27017')

    if  ticker and urls:
        logger.info(f'SAVE TO DB: {ticker}: Saving URLs')
        DB_NAME = 'news'
        COLLECTION_NAME = 'recommendations'
        db = client[DB_NAME]
        c = db[COLLECTION_NAME]
        
        processed_urls = c.find_one(
            {'ticker':ticker},
            {'news.url':1, '_id':0}
        )

        qty_stored_to_process_before = c.find_one(
            {'ticker':ticker},
            {'urls_to_process':1, '_id':0}
        )

        if qty_stored_to_process_before and len(qty_stored_to_process_before) > 0:
            qty_stored_to_process_before = len([
                _ for _ 
                in qty_stored_to_process_before['urls_to_process']
            ])
        else: qty_stored_to_process_before = 0
        
        if len(processed_urls) > 0:
            processed_urls = [
                _['url'] for _ 
                in processed_urls['news']
            ]
            to_process_urls = [
                _ for _ 
                in urls 
                if _ not in processed_urls
            ]
        else: to_process_urls = urls

        # $addToSet operator adds a value to an array 
        # unless the value is already present
        # $each modifier allows the $addToSet operator 
        # to add multiple values to the array 
        c.update_one(
            {'ticker' : ticker},
            {'$addToSet': 
                {'urls_to_process' : 
                    { '$each': to_process_urls}
                }
            },
            upsert = True
        )

        qty_stored_to_process_after = c.find_one(
            {'ticker':ticker},
            {'urls_to_process':1, '_id':0}
        )

        if len(qty_stored_to_process_after) > 0:
            qty_stored_to_process_after = len([
                _ for _ 
                in qty_stored_to_process_after['urls_to_process']
            ])
        else: qty_stored_to_process_after = 0

        logger.info(f'SAVE TO DB: {ticker}: Saved {qty_stored_to_process_after - qty_stored_to_process_before} new urls')
        return
    
    if urls:
        DB_NAME = 'news'
        COLLECTION_NAME = 'urls_to_process'
        db = client[DB_NAME]
        c = db[COLLECTION_NAME]

        for url in urls:
            c.update_one(
                {'url': url}, 
                {'$set': {'url': url}},
                upsert=True
            )

        logger.info(f'SAVE TO DB: NO TICKER: Saved to {COLLECTION_NAME}')
        return
    
    logger.info('SAVE TO DB: 0 URLs saved. No ticker and no URLs provided.')
    return False

def collect_all_urls(ticker):
        urls_finviz = get_urls_finviz(ticker)
        if urls_finviz: save_urls_to_db(urls_finviz, ticker=ticker)

        urls_yahoo = get_urls_yahoo(ticker)
        if urls_yahoo: save_urls_to_db(urls_yahoo, ticker=ticker)

def create_logger():
    import multiprocessing, logging
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '[%(asctime)s| %(levelname)s| %(processName)s] %(message)s',
        '%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler('logs/multi_url_extraction.log')
    handler.setFormatter(formatter)
    if not len(logger.handlers): 
        logger.addHandler(handler)
    return logger

if __name__ == '__main__': 
    from multiprocessing import Pool
    import datetime as dt
    from yahoo_fin import stock_info as si 
    logger = create_logger()

    logger.info(f'Starting URL extraction')

    # stop_urls =[
    #     'youtu.be',
    #     'youtube.com',
    #     'instagram.com'
    # ]

    p = Pool()

    start_date = (dt.datetime.today()-dt.timedelta(7)).strftime('%Y-%m-%d')
    urls_reddit = get_urls_reddit(start_date=start_date)
    if urls_reddit: 
        print(f'Saving {len(urls_reddit)} URLs from reddit')
        save_urls_to_db(urls_reddit)

    tickers = si.tickers_sp500()

    result = p.map_async(collect_all_urls, tickers)
    p.close()
    p.join()

    logger.info('Extraction finished')