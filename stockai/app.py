import streamlit as st
import pandas as pd
import altair as alt
from config.config import news_api_key
from data_collection.stocknewsapi_com import create_dataset

st.set_page_config(page_title='Stockai – stock price predictor', layout='wide')

if st.sidebar.button('Collect data'):
    data = create_dataset(
        news_api_key
    )
    st.write(data)