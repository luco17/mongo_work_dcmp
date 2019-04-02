import os, requests
from pymongo import MongoClient
from bson.regex import Regex

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

#Dot notation
##This allows you to access subdocuments within a document. In this example, accessing the prizes sub-doc
criteria = {'bornCountry': 'Austria', 'prizes.affiliations.country': {'$ne': 'Austria'}}
db.laureates.count_documents(criteria)

##Checking that there are laureates with no 'born' field
criteria = {'born': {'$exists': False}}
assert db.laureates.count_documents(criteria) == 0

##Finding laureates with more than two prizes
criteria = {'prizes.2': {'$exists': True}}
db.laureates.find_one(criteria)

#Using the DICTINCT method
##Checking for equality amongst category fields in laureates and prizes
assert set(db.prizes.distinct('category')) == set(db.laureates.distinct('prizes.category'))

##Countries that existed at the time of birth, but not at death
countries = set(db.laureates.distinct('diedCountry')) - set(db.laureates.distinct('bornCountry'))

##Counts of born countries, died countries and institution countries
len(db.laureates.distinct('bornCountry'))
len(db.laureates.distinct('diedCountry'))
len(db.laureates.distinct('prizes.affiliations.country'))

#Pre-filtering
##Laureates born in the USA but affiliated with another country
db.laureates.distinct('prizes.affiliations.country', {'bornCountry': 'USA'})

##Checking that multi-laureates don't exist in literature
criteria = {'laureates.2' : {'$exists': True}}
tp_cats = set(db.prizes.distinct('category', criteria))
assert set(db.prizes.distinct('category')) - tp_cats == {'literature'}

#Working with elem-matching to perform multiple filters
##Finding unshared prizes in physics since 1945, note the use of the $gte, $gt operators
criteria = {'prizes': {'$elemMatch': {
    'category': 'physics',
    'share': '1',
    'year': {'$gte': '1945'}
}}}

db.laureates.count_documents(criteria)

##Note the use of the $nin operator
unshared = {
    "prizes": {'$elemMatch': {
        'category': {'$nin': ["physics", "chemistry", "medicine"]},
        "share": "1",
        "year": {'$gte': "1945"},
    }}}

shared = {
    "prizes": {'$elemMatch': {
        'category': {'$nin': ["physics", "chemistry", "medicine"]},
        "share": {'$gt': "1"},
        "year": {'$gte': "1945"},
    }}}

##Ratio of unshared to shared laureates in disciplines other than phys, chem, med
db.laureates.count_documents(unshared) / db.laureates.count_documents(shared)

#Filtering with Regex
##Finding laureates whose first name begins with 'G'and surname begins with 'S'
db.laureates.count_documents({'firstname': Regex('^G'), 'surname': Regex('^S')})

##Germany filtering
###All mentions of Germany
criteria = {'bornCountry': Regex('Germany')}
db.laureates.distinct('bornCountry', criteria)

###Countries that were Germany but have reverted
criteria = {'bornCountry': Regex('^Germany' + ' \(' + 'now')}
db.laureates.distinct('bornCountry', criteria)

###Countries that were something else but are now Germany
criteria = {'bornCountry': Regex('now ' + 'Germany\)' + '$' )}
db.laureates.distinct('bornCountry', criteria)

##Printing the names of laureates who invented the transistor, using a list comprehension
criteria = {'prizes.motivation': Regex('transistor')}
first, last = 'firstname', 'surname'
[(laureate[first], laureate[last]) for laureate in db.laureates.find(criteria)]

#Projection (similar to selecting columns in a table)
##Returning a collection of only three fields, it returns a pymongo cursor so must be saved to list or set
list(db.laureates.find({}, {'firstname': 1, 'surname': 1, 'prizes.share': 1, '_id': 0}))[:3]

##below syntax returns the same, but can't deselect the _id field
list(db.laureates.find({}, ['firstname', 'surname', 'prizes.share']))[:3]
