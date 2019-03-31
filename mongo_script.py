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


#Looking at all DBs within the MongoClient
client.list_database_names()

#Looking at all the collections within nobel
client['nobel'].list_collection_names()

#Counting db record sizes
n_prizes = db.prizes.count_documents({})
n_laureates = db.laureates.count_documents({})

#Looking at individual documents
doc = db.prizes.find_one({})
doc2 = db.laureates.find_one({})

##Listing fields present in each document (note the need to use list)
db.prizes.find_one({}).keys()
db.laureates.find_one({}).keys()

#Filtering document counts
db.laureates.count_documents({
    'diedCountry': 'Russia'
})

##Filtering ops, less than
db.laureates.count_documents({'born': {'$lt' : '1800'}})

##extensive field filtering
criteria = {'diedCountry': 'USA', 'bornCountry': 'Germany', 'firstname': 'Albert'}
db.laureates.find_one(criteria)

##in operator
criteria = {'bornCountry': {'$in': ['USA', 'Mexico', 'Canada']}}
db.laureates.count_documents(criteria)

#not equal operator
criteria = {'bornCountry': {'$ne': 'USA'}, 'diedCountry': 'USA'}
db.laureates.count_documents(criteria)
