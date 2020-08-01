from config.config import create_logger
from data_collection.extract_text import scrape_urls
from yahoo_fin import stock_info as si 
from multiprocessing import Pool

if __name__ == '__main__':
    logger = create_logger('stockai.log')

    logger.info('::::::::Starting text extraction::::::::')

    tickers = si.tickers_sp500()
    # tickers = ['MSFT'] #for testing

    p = Pool()
    result = p.map_async(scrape_urls, tickers)

    p.close()
    p.join()

    logger.info(f'Finished text extraction')