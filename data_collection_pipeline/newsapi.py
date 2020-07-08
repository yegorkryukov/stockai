# use this code if ever purchase subscription to newsapi.org

import datetime as dt
from newsapi import NewsApiClient


from config.config import news_api_key
api = NewsApiClient(api_key=news_api_key)

query = ''
sources='bloomberg'
page_size = 100
page = 1
start_date = (dt.datetime.today()-dt.timedelta(30)).strftime('%Y-%m-%d')
end_date=(dt.datetime.today()).strftime('%Y-%m-%d')
sort_by = 'publishedAt'
articles = []

api_responce = api.get_everything(
    q=query,
    sources=sources,
#     domains='bloomberg.com',
    from_param=start_date,
    to=end_date,
    sort_by=sort_by,
    page_size=page_size,
    page=page
)

pages = api_responce['totalResults']//100
articles.extend(api_responce['articles'])
articles

pages

for page in range(2, pages+1):
    api_responce = api.get_everything(
        q=query,
        sources=sources,
    #     domains='bloomberg.com',
        from_param=start_date,
        to=end_date,
        sort_by=sort_by,
        page_size=page_size,
        page=page
    )
    articles.extend(api_responce['articles'])
    print(len(articles))

DB_NAME = 'news'
COLLECTION_NAME = 'urls_to_process_from_api'
db = client[DB_NAME]
c = db[COLLECTION_NAME]
for doc in all_articles['articles']:
    print(doc)
    c.update_one(
                {'url': doc['url']}, 
                {'$set': doc},
                upsert=True
            )
    break
