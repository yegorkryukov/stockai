def get_webdriver(browser=None, quit=False):
    """Selenium webdriver helper
    """
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    import logging

    if browser:
        return browser
    elif browser and quit:
        browser.quit()
        logging.info(':--- Quit headless browser')
    else:
        # profile = webdriver.FirefoxProfile()
        # profile.set_preference(
        #     'general.useragent.override', get_ua()
        # )
        options = Options()
        options.add_argument("--headless")
        browser = webdriver.Firefox(options=options, executable_path=r'geckodriver')
        logging.info(':--- Initialized headless browser')
        return browser

def get_ua():
    """Returns random browser User-Agent
    """
    from random import randint

    UA = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        'Mozilla/5.0 CK={} (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36'
    ]
    return UA[randint(0,len(UA)-1)]


def get_html_from_url(url):
    """Returns html content of a page at `url` 

    Parameters:
    --------
    url         : str, url to scrape
    
    Returns:
    --------
    response    : dict, {
            'status_code' : requests server response status code or None
            'html'        : str, html of the page or None
        }
    """
    import requests
    import validators 
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.common.exceptions import TimeoutException
    
    
    logger = create_logger()
    # logger.info(f'==-- Getting HTML from {url}')

    if validators.url(url):
        try:
            url_outline = f'https://outline.com/{url}'
            browser = get_webdriver()
            logger.info(f'==-- Trying OUTLINE.COM for {url_outline}')
            browser.get(url_outline)
            try:
                WebDriverWait(browser, 3).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'article-wrapper'))
                )
            except TimeoutException:
                logger.info(f'==-- WAITING FOR OUTLINE PAGE TO LOAD {url_outline}')
            finally:
                html = browser.page_source
            browser.quit()
            
            if "We're sorry. This page failed to Outline." in html:
                logger.info(f'==-- FAILED OUTLINE for {url_outline}')

                headers = {"User-Agent":get_ua()}
                requests.adapters.DEFAULT_RETRIES = 1

                url_google = f'https://www.google.com/search?&q=cache:{url}'
                logger.info(f'==-- Trying GOOGLE CACHE for {url_google}')
                url_get = requests.get(url_google, headers=headers, timeout=3)

                if 'class="g-recaptcha"' in url_get.text:
                    logger.info(f'==-- FAILED GOOGLE CACHE for {url_google}')
                    logger.info(f'==-- Trying ORIGINAL SOURCE for {url}')
                    url_get = requests.get(url, headers=headers, timeout=5)
                    
                    if url_get.status_code != 200:
                        logger.info(f'==-- BAD RESPONSE FOR {url}.\n STATUS: {url_get.status_code}')
                        return  {
                            'status_code' : url_get.status_code, 
                            'html'        : None
                        }
                html = url_get.text
            return {
                'status_code': True,
                'html' : html
            }
        except Exception as e:
            logger.info(f'==-- ERROR REQUESTING {url}.\n Error: {e}')
    else:
        logger.info(f'==-- NOT A VALID URL {url}')
        return  {
            'status_code' : None, 
            'html'        : None
        }
    

def scrape(url):
    """
    Scrapes an article from the 'url', extracts meta data using Nespaper3K package
    
    Parameters:
    --------
    url         : str, url to scrape
    
    Returns:
    --------
    doc         : dict,
        {
            'url'      : url,
            'date'     : article publish_date,
            'title'    : article title,
            'text'     : article cleaned_text,
            'keywords' : article meta_keywords,
            'summary'  : article summary
        }
    False       : bool, if get request fails or html < 500
    """
    from newspaper import Article, Config
    import re
    logger = create_logger()

    logger.info(f"==|| Trying extracting TEXT from {url}")
    config = Config()
    config.memoize_articles = False
    config.fetch_images = False
    config.language = 'en'
    config.browser_user_agent = get_ua()
    config.request_timeout = 5
    config.number_threads = 8

    response = get_html_from_url(url)

    if response['status_code'] and len(response['html']) > 500:
        try:
            article = Article(url=url, config=config)
            article.download_state = 2
            article.html = response['html']
            article.parse()
            article.nlp()
            
            if len((article.text).split()) > 200:
                logger.info(f'==|| Extracted TEXT from URL: {url}.\n Title: "{article.title}"')
                return {
                    'url'      : url,
                    'date'     : article.publish_date,
                    'title'    : article.title,
                    'text'     : " ".join(re.split(r'[\n\t]+', article.text)),
                    'keywords' : article.keywords,
                    'summary'  : article.summary
                }
            else:
                logger.info(
                    f'''==|| Could not extract TEXT from {url}.\n 
                    Article too short: {len(article.text.split())} words''')
        except Exception as e:
            logger.info(f'==|| Could not extract TEXT from {url}.\n Error: {e}')
    else:
        logger.info(f'==|| Could not extract TEXT from {url}')
    return False

def delete_url(ticker, url):
    import pymongo as pm
    logger = create_logger()
    client = pm.MongoClient('mongodb://localhost:27017')
    c = client['news']['recommendations']

    c.update_one(
        {'ticker' : ticker},
        {'$pull'  : {"urls_to_process" : url}}
    )

    logger.info(f'Deleted from DB URL {url}')

def save_scraped_meta(ticker, url, doc):
    """Saves doc to MongoDB news.recommendations.news for ticker and url

    Parameters
    ----------
    ticker    : str, ticker
    url       : str, url
    doc       : dict, keys: [url, date, title, text, keywords, summary]
    """
    import pymongo as pm
    logger = create_logger()
    client = pm.MongoClient('mongodb://localhost:27017')
    c = client['news']['recommendations']

    c.update_one(
        {'ticker'   : ticker},
        {'$addToSet': {'news' : doc}},
        upsert=True
    )
    logger.info(f"Saved title {doc['title']} for {url} to DB")

def scrape_urls(ticker):
    """Processes all urls stored in MongoDB document with `ticker.not_processed` field

    Parameters
    ----------
    ticker     : str, ticker to process

    Output
    ------
    Obtains list of urls from news.recommendations.ticker.urls_to_process, downloads html and saves
    meta data to news.recommendations.ticker.news document.
    """
    import pymongo as pm
    import time
    from random import uniform

    logger = create_logger()

    client = pm.MongoClient('mongodb://localhost:27017')
    c = client['news']['recommendations']

    logger.info(f'================>>>>>>>>>>Processing {ticker}<<<<<<<<===================')

    urls_to_process = c.find_one(
            {'ticker':ticker},
            {'urls_to_process':1, '_id':0}
        )

    if len(urls_to_process) > 0:
        urls_to_process = urls_to_process['urls_to_process']
        logger.info(f'Found {len(urls_to_process)} URLs to scrape')
    else:
        logger.info(f'No URLs found in {c} to process for {ticker}')
        return False
    
    scraped = 0
    for url in urls_to_process:
        wait = uniform(3,11)
        logger.info(f'||{ticker}||........Sleeping for {wait} seconds..........')
        time.sleep(wait)
        doc = scrape(url)
        if doc == 404:
            logger.info(f'URL {url} got 404. Deleting from DB')
            delete_url(ticker, url)
        elif doc:
            save_scraped_meta(ticker, url, doc)
            delete_url(ticker, url)
            scraped += 1
        else:
            logger.info(f'Did not get the text for {url}')
    
    logger.info(f'{ticker}: Scraped {scraped} our of {len(urls_to_process)} URLs')

def create_logger():
    import multiprocessing, logging
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '[%(asctime)s| %(levelname)s| %(processName)s] %(message)s',
        '%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler('logs/multi_text_extraction.log')
    handler.setFormatter(formatter)
    if not len(logger.handlers): 
        logger.addHandler(handler)
    return logger

# Start MongoDB
# !brew services start mongodb-community@4.2

# Stop MongoDB
# !brew services stop mongodb-community@4.2

if __name__ == '__main__': 
    from yahoo_fin import stock_info as si 
    from multiprocessing import Pool
    logger = create_logger()

    logger.info('Starting text extraction')

    p = Pool()
    tickers = si.tickers_sp500()
    result = p.map_async(scrape_urls, tickers)
    p.close()
    p.join()

    logger.info(f'Finished text extraction')