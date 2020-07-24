import progressbar

def fix_date_field(ticker):
    """Renaming 'date' field to 'datetime' for the `ticker`
    """
    news = c.find_one({'ticker':ticker})['news']
    qty_news = len(news)
    bar = progressbar.ProgressBar().start(max_value=qty_news, init=False)
    print(f'{ticker}: found {qty_news} records.')
    fixed = 0
    for i, doc in enumerate(news):
        bar.update(i+1)
        if 'date' in doc.keys():
            c.update_one(
                {'ticker'   : ticker},
                {'$pull': {'news' :{ 'url': doc['url'] }}},
            )
            doc['datetime'] = doc['date']
            doc.pop('date')
            c.update_one(
                    {'ticker'   : ticker},
                    {'$addToSet': {'news' : doc}},
                    upsert=True
                )
            fixed +=1
    bar.finish()
    print(f'{ticker}: updated {fixed} records')

from yahoo_fin import stock_info as si 
tickers = si.tickers_sp500()
# tickers = ['A']
for t in tickers: fix_date_field(t)
