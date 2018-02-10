import pandas as pd
import sys, os
import sqlalchemy as sa
from newsapi import NewsApiClient

def get_and_count(requestcount, **kwargs):
    everything = newsapi.get_everything(**kwargs)
    requestcount += 1
    return everything, requestcount

def get_last_article(sqlite):
    ''' Find last article by looking at last source id and last date '''
    print('Getting last article')
    # Find last source
    last_source = pd.read_sql("select max(source) source from all_articles", sqlite).iloc[0,0]
    # Find last date
    last_date = pd.read_sql("select min(publishedAt) publishedAt from all_articles where source=='{}'".format(last_source),
                            sqlite, parse_dates=['publishedAt']).iloc[0,0]

    return last_source, last_date


if __name__=='__main__':
    newsapi = NewsApiClient(api_key='07be0d6ef4144691bf9075df390b2ef1')

    df_sources = pd.read_csv('sources.csv')
    df_sources_en = df_sources.query("language=='en'").sort_values('id').set_index('id')

    fromdate = sys.argv[1]
    todate = sys.argv[2]
    totalrequests = int(sys.argv[3])
    fname = sys.argv[4]

    if os.path.exists(fname):
        sqlite = sa.create_engine('sqlite:///{}'.format(fname)).connect()
        last_source, last_date = get_last_article(sqlite)
        todate = last_date.strftime('%Y-%m-%d %H:%M:%S')
        df_sources_en = df_sources_en.loc[last_source:]
    else:
        sqlite = sa.create_engine('sqlite:///{}'.format(fname)).connect()

    ids = df_sources_en.index

    requestcount = 0
    for id in ids:
        if requestcount > totalrequests: break
        print('Getting current source from', id)
        everything, requestcount = get_and_count(requestcount, sources=[id], from_parameter=fromdate, to=todate,
                                                 sort_by='publishedAt', page_size=1)
        totalResults = everything['totalResults']
        numresults = 0
        page = 1
        while (numresults < totalResults) & (requestcount <= totalrequests):
            print(requestcount)
            everything, requestcount = get_and_count(requestcount, sources=[id], from_parameter=fromdate, to=todate,
                                                     sort_by='publishedAt', page_size=100, page=page)

            articles_df = pd.DataFrame(everything['articles'])
            articles_df.source = articles_df.source.apply(lambda x: x['id'])
            articles_df.publishedAt = pd.to_datetime(articles_df.publishedAt)
            if numresults == 0:
                cur_source_articles = articles_df
            else:
                cur_source_articles = cur_source_articles.append(articles_df, ignore_index=True)
            cur_source_articles.drop_duplicates(inplace=True)

            if (requestcount % 10 == 0) & (requestcount > 0):
                print('Writing to', fname)
                cur_source_articles.to_sql('all_articles', sqlite, index=False, if_exists='append')

            numresults += articles_df.shape[0]
            page += 1


