import os, requests
from pymongo import MongoClient

#Connecting to MongoClient
client = MongoClient()
db = client['nobel']

#Reading from the API
for collection_name in ['prizes', 'laureates']:
    singular = collection_name[:-1]
    response = requests.get(
        "http://api.nobelprize.org/v1/{}.json".format(singular))
    documents = response.json()[collection_name]
    db[collection_name].insert_many(documents)

assert client.nobel == db
assert db.prizes == db['prizes']

#Creating the collections
n_prizes = db.prizes.count_documents({})
n_laureates = db.laureates.count_documents({})

#Looking at individual documents
doc = db.prizes.find_one({})
doc2 = db.laureates.find_one({})

#Filtering
db.laureates.count_documents({
    'diedCountry': 'Russia'
})

db.laureates.find({
    'diedCountry': 'Russia'
})
