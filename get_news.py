import pandas as pd
import numpy as np
from newsapi import NewsApiClient

newsapi = NewsApiClient(api_key='07be0d6ef4144691bf9075df390b2ef1')

df_sources = pd.read_csv('sources.csv')
df_sources_en = df_sources.query("language=='en'")

fromdate = '2017-01-01'
todate = '2018-02-07'

requestcount = 0
totalrequests = 100

def get_and_count(requestcount, **kwargs):
    everything = newsapi.get_everything(**kwargs)
    requestcount += 1
    return everything, requestcount

while requestcount < totalrequests:
    for i, id in enumerate(df_sources_en.id):
        everything, requestcount = get_and_count(requestcount, sources=[id],from_parameter=fromdate,to=todate,sort_by='publishedAt',page_size=1)
        totalResults = everything['totalResults']
        j = 0
        page = 1
        while j < totalResults:
            everything, requestcount = get_and_count(requestcount, sources=[id],from_parameter=fromdate,to=todate,sort_by='publishedAt',page_size=100,page=page)
            articles_df = pd.DataFrame(everything['articles'])
            articles_df.source = articles_df.source.apply(lambda x: x['id'])
            articles_df.publishedAt = pd.to_datetime(articles_df.publishedAt)
            if j == 0:
                cur_source_articles = articles_df
            else:
                cur_source_articles = cur_source_articles.append(articles_df, ignore_index=True)
            cur_source_articles.drop_duplicates(inplace=True)

            j += articles_df.shape[0]
            page += 1
        if i == 0:
            all_articles = cur_source_articles
        else:
            all_articles = all_articles.append(cur_source_articles, ignore_index=True)