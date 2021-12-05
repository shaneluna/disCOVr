# %%
# module imports
import pandas
import requests


# %%
# import data
df_news = pandas.read_csv('data/recovery-news-data.csv', usecols = ['news_id', 'url', 'title', 'body_text'])


# %%
# fakebox api function
endpoint = 'http://192.168.1.5:8880/fakebox/check'
headers = {'Content-Type': 'application/json; charset=utf-8'}
keys = ('decision', 'score')

def score_bias(row):
    news_id, url, title, body_text = row
    data = {'url': url, 'title': title, 'content': body_text}
    response = requests.post(
        endpoint,
        data = data
    )
    status = [response.status_code, response.json()['success']]
    title = [response.json()['title'].get(key) for key in keys]
    content = [response.json()['content'].get(key) for key in keys]
    return pandas.Series({
        'news_id': news_id,
        'status_code': status[0],
        'success': status[1],
        'title_decision': title[0],
        'title_score': title[1],
        'content_decision': content[0],
        'content_score': content[1]
    })


# %%
# analyze articles
df_biases = df_news.apply(score_bias, axis = 1)


# %%
# saving to file
df_biases.to_csv(f'data/news_biases.csv', index = False)

