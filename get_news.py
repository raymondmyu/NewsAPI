import pandas as pd
import sys, os
from newsapi import NewsApiClient

def get_and_count(requestcount, **kwargs):
    everything = newsapi.get_everything(**kwargs)
    requestcount += 1
    return everything, requestcount

if __name__=='__main__':
    newsapi = NewsApiClient(api_key='07be0d6ef4144691bf9075df390b2ef1')

    df_sources = pd.read_csv('sources.csv')
    df_sources_en = df_sources.query("language=='en'")

    fromdate = sys.argv[1]
    todate = sys.argv[2]
    totalrequests = int(sys.argv[3])
    fname = sys.argv[4]

    requestcount = 0
    for id in df_sources_en.id:
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
                if os.path.exists(fname):
                    cur_source_articles.to_csv(fname, mode='a', header=False, index=False)
                else:
                    cur_source_articles.to_csv(fname, index=False)

            numresults += articles_df.shape[0]
            page += 1


