import streamlit as st
import pandas as pd
import altair as alt
from config.config import news_api_key
from data_collection.stocknewsapi_com import create_dataset
from misc.plotting import plot_ticker_data
from gensim.utils import simple_preprocess
from collections import defaultdict
from gensim import corpora
import logging
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression


logger = logging.getLogger()
st.set_page_config(page_title='Stockai – stock price predictor', layout='wide')
col1, col2 = st.beta_columns(2)

ticker = 'AAPL'

stoplist = set('for a of the and to in'.split())

data = create_dataset(
    news_api_key,
    ticker
)
col1.plotly_chart(plot_ticker_data(data))


# simple text cleaning and tokenizing
texts = [
    [word for word in simple_preprocess(text) if word not in stoplist]
    for text in data['text']
]

frequency = defaultdict(int)

for text in texts:
    for token in text:
        frequency[token] += 1

# Only keep words that appear more than once
texts = [
    [token for token in text if frequency[token] > 1] 
    for text in texts
]

# vectorize with BOW model
dictionary = corpora.Dictionary(texts)
bow_texts = [dictionary.doc2bow(text) for text in texts]
data['vec_text'] = bow_texts
data['clean_text'] = [' '.join(text) for text in texts]

vectorizer = TfidfVectorizer()
model = LinearRegression()
data['tfidf'] = vectorizer.fit_transform(data['clean_text'])

# train, test split
cols = ['tfidf', 'adjclose']
train_data, test_data = data[cols].iloc[0:int(len(data)*0.7), :], data[cols].iloc[int(len(data)*0.7):, :]

X, y = vectorizer.fit_transform([' '.join(text) for text in texts]), data.adjclose
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, shuffle=False)

model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# plot train and test data
f, ax = plt.subplots(figsize=(10,5))
plt.title(f'{ticker} adjclose prices')
plt.xlabel('Date')
plt.ylabel('Price')
plt.plot(train_data.adjclose, 'blue', label='Training Data')
plt.plot(test_data.adjclose, 'green', label='Testing Data')
plt.plot(test_data.index, y_pred, label='Predicted Value')
plt.legend();
col2.pyplot(f)

# data.to_csv('data.csv', index=False)

with st.beta_expander(f'Data ({len(data)} rows)'):
    st.write(data.head())