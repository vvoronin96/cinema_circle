import pandas as pd
import numpy as np
import re
from pymongo import MongoClient

year_begin = 1920
year_end = 2021


def array_wsum(arr):
    if isinstance(arr, list):
        la = len(arr)
        s = 0
        for i in range(1, la):
            s += i * arr[i]
        return s
    else:
        return None


def array_msum(arr):
    if isinstance(arr, list):
        la = len(arr)
        sa = sum(arr)
        return sa * (la - 1)
    else:
        return None


def data_to_agg(dt, groupby):
    dt_ = dt.groupby(groupby, as_index=False).agg({'title': 'count',
                                                   'runtimes': 'mean',
                                                   'rating': 'mean',
                                                   'votes': ['mean', 'sum'],
                                                   'budget': ['mean', 'sum'],
                                                   'gross': ['mean', 'sum'],
                                                   'budget fair': 'sum',
                                                   'gross fair': 'sum',
                                                   'profit': 'mean',
                                                   'screen width': 'mean',
                                                   'Sex & Nudity wsum': 'sum',
                                                   'Violence & Gore wsum': 'sum',
                                                   'Profanity wsum': 'sum',
                                                   'Alcohol, Drugs & Smoking wsum': 'sum',
                                                   'Frightening & Intense Scenes wsum': 'sum',
                                                   'Sex & Nudity msum': 'sum',
                                                   'Violence & Gore msum': 'sum',
                                                   'Profanity msum': 'sum',
                                                   'Alcohol, Drugs & Smoking msum': 'sum',
                                                   'Frightening & Intense Scenes msum': 'sum', })
    dt_.columns = groupby + ['cnt', 'runtimes', 'rating', 'votes mean', 'votes sum', 'budget mean', 'budget sum',
                             'gross mean', 'gross sum', 'budget fair', 'gross fair', 'profit mean', 'screen width',
                             'Sex & Nudity wsum', 'Violence & Gore wsum', 'Profanity wsum',
                             'Alcohol, Drugs & Smoking wsum', 'Frightening & Intense Scenes wsum',
                             'Sex & Nudity msum', 'Violence & Gore msum', 'Profanity msum',
                             'Alcohol, Drugs & Smoking msum', 'Frightening & Intense Scenes msum']
    dt_['budget fair'] = dt_['budget fair'].replace(0, np.nan)
    dt_['gross fair'] = dt_['gross fair'].replace(0, np.nan)

    dt_['profit'] = dt_['gross sum'] / dt_['budget sum']

    dt_['Sex & Nudity'] = dt_['Sex & Nudity wsum'] / dt_['Sex & Nudity msum']
    dt_['Violence & Gore'] = dt_['Violence & Gore wsum'] / dt_['Violence & Gore msum']
    dt_['Profanity'] = dt_['Profanity wsum'] / dt_['Profanity msum']
    dt_['Alcohol, Drugs & Smoking'] = dt_['Alcohol, Drugs & Smoking wsum'] / dt_['Alcohol, Drugs & Smoking msum']
    dt_['Frightening & Intense Scenes'] = dt_['Frightening & Intense Scenes wsum'] / dt_[
        'Frightening & Intense Scenes msum']

    dt_ = dt_[groupby + ['cnt', 'runtimes', 'rating', 'votes mean', 'votes sum', 'budget mean', 'budget sum',
                         'gross mean', 'gross sum', 'profit mean', 'profit', 'screen width', 'Sex & Nudity',
                         'Violence & Gore', 'Profanity', 'Alcohol, Drugs & Smoking', 'Frightening & Intense Scenes']]

    return dt_

def connect_to_db(local=True):

    login = 'clientreader'
    pwd = 'iamclientreader1488'
    if local:
        local_db = f'mongodb://{login}:{pwd}@mongo:27017/'
        print(f"Trying to connect to {local_db}\n")
        client = MongoClient(local_db)
        try:
           # The ismaster command is cheap and does not require auth.
           client.admin.command('ismaster')
           print("Connected to local MongoDB")
        except ConnectionFailure:
           print("Server not available")
    else:
        client = MongoClient(
            "mongodb+srv://" + login + ":" + pwd + "@clusterimdb.kihfk.mongodb.net/ClusterIMDb?retryWrites=true&w=majority")

        print("Connected cluster to MongoDB")

    return client


def update_data():

    client = connect_to_db()

    db = client['IMDb']
    collection_films = db['Films']

    films = [f for f in collection_films.find({'year': {'$gt': year_begin, '$lt': year_end}})]

    for f in films:
        if 'box office' in f:
            if 'Budget' in f['box office']:
                if '$' in f['box office']['Budget']:
                    f['budget'] = int(re.sub('[^0-9]', '', f['box office']['Budget'].split(' ')[0]))
            if 'Cumulative Worldwide Gross' in f['box office']:
                if '$' in f['box office']['Cumulative Worldwide Gross']:
                    f['gross'] = int(re.sub('[^0-9]', '', f['box office']['Cumulative Worldwide Gross'].split(' ')[0]))

    columns = ['title', 'year', 'genres', 'languages', 'runtimes', 'rating', 'votes', 'budget', 'gross', 'aspect ratio',
            'Sex & Nudity', 'Violence & Gore', 'Profanity', 'Alcohol, Drugs & Smoking', 'Frightening & Intense Scenes']
    data = pd.DataFrame(films)[columns].explode('runtimes')
    data['runtimes'] = data['runtimes'].astype(float)

    data['profit'] = data['gross'] / data['budget']
    data['gross fair'] = np.where(np.isnan(data['budget']), np.nan, data['gross'])
    data['budget fair'] = np.where(np.isnan(data['gross']), np.nan, data['budget'])

    data['aspect ratio'] = data['aspect ratio'].str.replace(',', '.'). \
        str.replace('[^0-9:. x]', ''). \
        str.replace(' :', ':').str.replace(': ', ':'). \
        str.replace('1920x1080', '1.78:1'). \
        str.replace('4096 x 2160', '1.9:1'). \
        str.replace('16:9', '1.78:1'). \
        str.replace('21:9', '2.33:1'). \
        str.replace('4:3', '1.33:1'). \
        str.replace('(:)([0-9]{2})', '.\\2'). \
        str.replace('(.)([0-9]{2})(.)', '.\\2:')
    data['screen width'] = pd.to_numeric(data['aspect ratio'].str.split(':', expand=True)[0]. \
        str.replace(' ', '').replace('', np.nan),errors='coerce') 
    data['Sex & Nudity wsum'] = data['Sex & Nudity'].apply(array_wsum)
    data['Violence & Gore wsum'] = data['Violence & Gore'].apply(array_wsum)
    data['Profanity wsum'] = data['Profanity'].apply(array_wsum)
    data['Alcohol, Drugs & Smoking wsum'] = data['Alcohol, Drugs & Smoking'].apply(array_wsum)
    data['Frightening & Intense Scenes wsum'] = data['Frightening & Intense Scenes'].apply(array_wsum)

    data['Sex & Nudity msum'] = data['Sex & Nudity'].apply(array_msum)
    data['Violence & Gore msum'] = data['Violence & Gore'].apply(array_msum)
    data['Profanity msum'] = data['Profanity'].apply(array_msum)
    data['Alcohol, Drugs & Smoking msum'] = data['Alcohol, Drugs & Smoking'].apply(array_msum)
    data['Frightening & Intense Scenes msum'] = data['Frightening & Intense Scenes'].apply(array_msum)

    data['Sex & Nudity'] = data['Sex & Nudity wsum'] / data['Sex & Nudity msum']
    data['Violence & Gore'] = data['Violence & Gore wsum'] / data['Violence & Gore msum']
    data['Profanity'] = data['Profanity wsum'] / data['Profanity msum']
    data['Alcohol, Drugs & Smoking'] = data['Alcohol, Drugs & Smoking wsum'] / data['Alcohol, Drugs & Smoking msum']
    data['Frightening & Intense Scenes'] = data['Frightening & Intense Scenes wsum'] / data[
        'Frightening & Intense Scenes msum']

    group = 'genres'

    datag = data.explode(group)
    gdatag = data_to_agg(datag, ['year', group])

    gdatag.to_pickle('data.pkl', compression='gzip')



if __name__ == '__main__':
    update_data()
