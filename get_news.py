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

    # fromdate, todate, totalrequests, fname = '2018-02-09', '2018-02-09', 100, '2018-02-09to2018-02-09.db'

    fromdate, todate, totalrequests, fname = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]

    sqlite = sa.create_engine('sqlite:///{}'.format(fname)).connect()

    if sqlite.execute("select name from sqlite_master where type=='table' and name=='all_articles'").fetchone():
        last_source, last_date = get_last_article(sqlite)
        todate = last_date.strftime('%Y-%m-%d %H:%M:%S')
        df_sources_en = df_sources_en.loc[last_source:]

    ids = df_sources_en.index

    requestcount = 0
    for id in ids:
        print('Getting current source from', id)
        # Find total number of Results
        everything, requestcount = get_and_count(requestcount, sources=[id], from_parameter=fromdate, to=todate,
                                                 sort_by='publishedAt', page_size=100, page=1)
        totalResults = everything['totalResults']
        print('Total Results:',totalResults)

        numresults = 1
        page = 1
        while (numresults <= totalResults) & (requestcount <= totalrequests):
            print(requestcount)
            if page > 1:
                everything, requestcount = get_and_count(requestcount, sources=[id], from_parameter=fromdate, to=todate,
                                                         sort_by='publishedAt', page_size=100, page=page)
            if 'articles' not in everything.keys():
                page += 1
                continue
            elif len(everything['articles']) == 0:
                break

            articles_df = pd.DataFrame(everything['articles'])
            articles_df.source = articles_df.source.apply(lambda x: x['id'])
            articles_df.publishedAt = pd.to_datetime(articles_df.publishedAt)
            if numresults == 1:
                cur_source_articles = articles_df
            else:
                cur_source_articles = cur_source_articles.append(articles_df, ignore_index=True)
            cur_source_articles.drop_duplicates(inplace=True)

            numresults += articles_df.shape[0]
            page += 1

        print('Writing to sqlite')
        cur_source_articles.to_sql('all_articles', sqlite, index=False, if_exists='append')

        if requestcount > totalrequests:
            print('Removing duplicates from sqlite')
            sqlite.execute("create table all_articles_temp as select distinct * from all_articles")
            sqlite.execute("drop table all_articles")
            sqlite.execute("create table all_articles as select * from all_articles_temp")
            sqlite.execute("drop table all_articles_temp")
            break



