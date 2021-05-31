import pandas as pd
import numpy as np
import re
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from tqdm import tqdm

year_begin = 1920
year_end = 2021

login = 'clientreader'
pwd = 'iamclientreader1488'

print('Started python script')
client = MongoClient(
    "mongodb+srv://" + login + ":" + pwd + "@clusterimdb.kihfk.mongodb.net/ClusterIMDb?retryWrites=true&w=majority")
print("Connected to online MongoDB\n\n")
local_db = f'mongodb://{login}:{pwd}@mongo:27017/'
print(f"Trying to connect to {local_db}\n")
client_local = MongoClient(local_db)
try:
   # The ismaster command is cheap and does not require auth.
   client_local.admin.command('ismaster')
   print("Connected to local MongoDB")
except ConnectionFailure:
   print("Server not available")



db = client['IMDb']
collection_films = db['Films']
running = True
while running:
    try:
        print("Getting all films")
        films = [f for f in tqdm(collection_films.find({'year': {'$gt': year_begin, '$lt': year_end}}))]
        print("Got all films")
        running = False
    except Exception as e:
        print(f"Got an error: {e} \n\n Trying one more time")


db_local = client_local['IMDb']
collection_films = db_local['Films']


try:
    print("If base didn't empty, trying to remove collections")
    collection_films.delete_many({})
    print("DB is clear")
except Exception as e:
    print(f"Got an error: {e}")


running = True
while running:
    try:
        print("Insert into local MongoDB")
        for film in films:
            collection_films.insert_one(film)
        running = False
    except Exception as e:
        print(f"Got an error: {e} \n\n Trying one more time")
print('\nAll done\n')