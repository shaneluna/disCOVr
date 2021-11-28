# %%
# module imports
import logging
import os
import pandas
from google.cloud import language_v1


# %%
# environment variable export
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'secrets/discovr-2021-c63b2ee5e9f6.json'


# %%
# google client creation
gclient = language_v1.LanguageServiceClient()


# %%
# reading news articles
df_news = pandas.read_csv('data/recovery-news-data.csv')
df_news.drop('Unnamed: 0', axis = 1, inplace = True)

df_text = df_news[['news_id', 'body_text']].copy(deep = True)


# %%
# api function
class GoogleAPI:
    def __init__(self):
        # dataframes to store query results
        self.df_categories = pandas.DataFrame(columns = ['news_id', 'category', 'confidence'])
        self.df_entities = pandas.DataFrame(columns = ['news_id', 'entity', 'salience'])
        self.df_sentiment = pandas.DataFrame(columns = ['news_id', 'score', 'magnitude'])

        # gclient feature options
        self.features = language_v1.AnnotateTextRequest.Features(
            extract_syntax = False,
            extract_entities = True,
            extract_document_sentiment = True,
            extract_entity_sentiment = True,
            classify_text = True
        )

    def query_tuple(self, tup):
        # breaking out tuple components
        self.news_id = tup.news_id
        text = tup.body_text

        # assembling document to send
        document = language_v1.Document(
            content = text,
            type_ = language_v1.Document.Type.PLAIN_TEXT
        )

        # query response object
        self.response = gclient.annotate_text(
            request = {
                'document': document,
                'features': self.features
            }
        )
    
    def store_results(self):
        # storing categories
        for cat in self.response.categories:
            row = len(self.df_categories)
            self.df_categories.loc[row] = [self.news_id, cat.name, cat.confidence]
        
        # storing entities
        thres = .005
        is_above_thres = lambda x: x.salience > thres
        top_entities = list(filter(is_above_thres, self.response.entities))
        id_name_salience = [(self.news_id, x.name, x.salience) for x in top_entities]
        df_results = pandas.DataFrame(id_name_salience, columns = ['news_id', 'entity', 'salience'])
        self.df_entities = self.df_entities.append(df_results)

        # storing sentiment
        row = len(self.df_sentiment)
        magnitude = self.response.document_sentiment.magnitude
        score = self.response.document_sentiment.score
        self.df_sentiment.loc[row] = [self.news_id, score, magnitude]


# %%
# initializing google api
results = GoogleAPI()


# %%
# looping through articles
for tup in df_text.itertuples():
    results.query_tuple(tup)
    results.store_results()


# %%
# saving results to disk
results.df_categories.to_csv('data/news_categories.csv', index = False)
results.df_entities.to_csv('data/news_entities.csv', index = False)
results.df_sentiment.to_csv('data/news_sentiments.csv', index = False)

