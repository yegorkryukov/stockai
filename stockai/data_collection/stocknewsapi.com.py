import pymongo as pm
client = pm.MongoClient('mongodb://localhost:27017')

DB_NAME = 'news'
COLLECTION_NAME = 'recommendations'
db = client[DB_NAME]
c = db[COLLECTION_NAME]

url = 'https://stocknewsapi.com/api/v1/category'
page = 201
tickers = 'AAPL'
news = []

while True:
    print(f'Processing page {page}')
    params = {
        'section':'alltickers',
        'index':'SP500',
        'token':'jfcfxkqq1wtpdprododvo1wds1gppzpm1wg0gh5t',
        'items':50,
        'type':'article',
        'date':'01012019-today',
        'country':'USA',
        'page':page
    }
    
    r = requests.get(url=url, params=params)
    results = r.json()
    total_articles = len(results['data'])
    print(f'Got {total_articles} results')
    
    if total_articles > 0:
        articles = results['data']
        news.extend(articles)
        for a in articles:
            tickers = a['tickers']
            print(f'Got these tickers attached to the article: {tickers}')
            a_url = a['news_url']
            for ticker in tickers:
                print(f'Saving {ticker}. URL: {a_url}')
                c.update_one(
                    {'ticker' : ticker},
                    {'$addToSet':
                        {'urls_to_process' : a_url}
                    },
                    upsert = True
                )
        page+=1
    else:
        print(f"End of news. Len: {total_articles}. Server message: {results['error']}")
        break
print(f'Got a total of {len(news)} articles.')
