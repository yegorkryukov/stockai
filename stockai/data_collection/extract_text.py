from config.config import create_logger, get_ua

logger = create_logger('text_extraction.log')

def get_webdriver(browser=None, quit=False):
    """Selenium webdriver helper
    """
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options

    if browser:
        return browser
    elif browser and quit:
        browser.quit()
        logger.info(':--- Quit headless browser')
    else:
        profile = webdriver.FirefoxProfile()
        profile.set_preference(
            'general.useragent.override', get_ua()
        )
        options = Options()
        options.add_argument("--headless")
        browser = webdriver.Firefox(options=options, executable_path=r'geckodriver')
        logger.info(':--- Initialized headless browser')
        return browser

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

    logger.info(f'get_html_from_url: {url}')

    if validators.url(url):
        try:
            url_outline = f'https://outline.com/{url}'
            browser = get_webdriver()
            logger.info(f'get_html_from_url: Trying OUTLINE.COM for {url_outline}')
            browser.get(url_outline)
            try:
                WebDriverWait(browser, 3).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'article-wrapper'))
                )
            except TimeoutException:
                logger.info(f'get_html_from_url: WAITING FOR OUTLINE PAGE TO LOAD {url_outline}')
            finally:
                html = browser.page_source
                logger.info(f'get_html_from_url: got html with {len(html.split())} words for {url_outline}')
            browser.quit()
            
            if "We're sorry. This page failed to Outline." in html:
                logger.info(f'get_html_from_url:  FAILED OUTLINE for {url_outline}')

                headers = {"User-Agent": get_ua()}

                logger.info(f'get_html_from_url: Trying ORIGINAL SOURCE for {url}')
                url_get = requests.get(url, headers=headers, timeout=5)
                
                if url_get.status_code != 200:
                    logger.info(f'get_html_from_url: BAD RESPONSE FOR {url}\n STATUS: {url_get.status_code}')
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
            logger.info(f'get_html_from_url: ERROR REQUESTING {url}\n Error: {e}')
    else:
        logger.info(f'get_html_from_url: NOT A VALID URL {url}')
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

    logger.info(f"SCRAPE: trying {url}")
    config = Config()
    config.memoize_articles = False
    config.fetch_images = False
    config.language = 'en'
    config.browser_user_agent = get_ua()
    config.request_timeout = 5
    config.number_threads = 8

    response = get_html_from_url(url)

    if response['status_code'] and response['html']:
        try:
            article = Article(url=url, config=config)
            article.download_state = 2
            article.html = response['html']
            article.parse()
            article.nlp()

            words_count = len((article.text).split())
            
            if words_count > 200:
                logger.info(f'SCRAPE: Extracted TEXT from URL: {url}\n Title: "{article.title}"')
                return {
                    'url'      : url,
                    'datetime' : article.publish_date,
                    'title'    : article.title,
                    'text'     : " ".join(re.split(r'[\n\t]+', article.text)),
                    'keywords' : article.keywords,
                    'summary'  : article.summary
                }
            else:
                logger.info(
                    f'''SCRAPE: Could not extract TEXT from {url}\n 
                    Article too short: {words_count} words''')
        except Exception as e:
            logger.info(f'SCRAPE: Could not extract TEXT from {url}\n Error: {e}')
    else:
        logger.info(f'SCRAPE: Could not extract TEXT from {url}')
    return False

def delete_url(ticker, url):
    import pymongo as pm
    client = pm.MongoClient('mongodb://localhost:27017')
    c = client['news']['recommendations']

    c.update_one(
        {'ticker' : ticker},
        {'$pull'  : {"urls_to_process" : url}}
    )

    logger.info(f'{ticker}| Deleted from DB URL {url}')

def save_scraped_meta(ticker, url, doc):
    """Saves doc to MongoDB news.recommendations.news for ticker and url

    Parameters
    ----------
    ticker    : str, ticker
    url       : str, url
    doc       : dict, keys: [url, date, title, text, keywords, summary]
    """
    import pymongo as pm
    client = pm.MongoClient('mongodb://localhost:27017')
    c = client['news']['recommendations']

    c.update_one(
        {'ticker'   : ticker},
        {'$addToSet': {'news' : doc}},
        upsert=True
    )
    logger.info(f"{ticker}| Saved title {doc['title']} for {url} to DB")

def scrape_urls(ticker):
    """Processes all urls stored in MongoDB document with `ticker.urls_to_process` field

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

    client = pm.MongoClient('mongodb://localhost:27017')
    c = client['news']['recommendations']

    logger.info(f'================>>>>>>>>>>Processing {ticker}<<<<<<<<===================')

    urls_to_process = c.find_one(
            {'ticker':ticker},
            {'urls_to_process':1, '_id':0}
        )

    if len(urls_to_process) > 0:
        urls_to_process = urls_to_process['urls_to_process']
        logger.info(f'{ticker}| Found {len(urls_to_process)} URLs to scrape')
    else:
        logger.info(f'{ticker}| No URLs found in {c} to process for {ticker}')
        return False
    
    scraped = 0
    for url in urls_to_process:
        doc = scrape(url)
        if doc == 404:
            logger.info(f'{ticker}| URL {url} got 404. Deleting from DB')
            delete_url(ticker, url)
        elif doc:
            logger.info(f'{ticker}| URL {url} got doc to save')
            save_scraped_meta(ticker, url, doc)
            delete_url(ticker, url)
            scraped += 1
        else:
            logger.info(f'{ticker}| Did not get the text for {url}')
        wait = uniform(3,7)
        logger.info(f'{ticker}| ........Sleeping for {wait} seconds..........')
        time.sleep(wait)

    
    logger.info(f'{ticker}: Scraped {scraped} our of {len(urls_to_process)} URLs')
