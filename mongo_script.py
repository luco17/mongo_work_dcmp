import os, requests, itertools
from pymongo import MongoClient
from bson.regex import Regex
from fractions import Fraction
from operator import itemgetter
from collections import Counter, OrderedDict
from pprint import pprint
from itertools import groupby

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
criteria = {'bornCountry': Regex('now ' + 'Germany\)' + '$' )}system
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

##Using a projection to find only relevant laureates with G firstnames and S surnames, returns a list
names = [' '.join([doc['firstname'], doc['surname']])
            for doc in db.laureates.find(
                {'firstname': {'$regex': '^G'},
                'surname': {'$regex': '^S'}},
                ['firstname', 'surname'])]

##Checking that all fractional prizes add up to one
docs = list(db.prizes.find({}, ['laureates.share']))
check = all(1 == sum(Fraction(1, int(laureate['share'])) for laureate in doc['laureates']) for doc in docs)
assert check

##Sorting
###Basic sorted list
docs = list(db.laureates.find(
    {'born': {'$gte': '1900'}, 'prizes.year': {'$gte': '1954'}},
    {'born': 1, 'prizes.year': 1, '_id': 0},
    sort = [('prizes.year', 1), ('born', -1)]))

###More advanced Sorting
docs = list(db.prizes.find(
    {'category': 'physics'},
    {'year': 1, 'laureates.firstname': 1, 'laureates.surname': 1, '_id': 0},
    sort = [('year', 1)]))

###More precision in specification and use of the .format feature
for doc in sorted(docs, key = itemgetter('year')):
    print('{year}: {fst_lte_snme}'.format(year = doc['year'], fst_lte_snme = doc['laureates'][0]['surname']))

###Finding years where a given prize was missing, has to run as one block, otherwise must use cursor.rewind
orig_cats = set(db.prizes.distinct('category', {'year': '1901'}))
cursor = db.prizes.find(
    {'category': {'$in': list(orig_cats)}},
    ['category', 'year'],
    sort = [('year', -1), ('category', 1)])
cursor.rewind()

not_awarded = []

for key, group in itertools.groupby(cursor, key = itemgetter('year')):
    year_categories = set(prize['category'] for prize in group)
    missing = ', '.join(sorted(orig_cats - year_categories))
    if missing: not_awarded.append("{}: {}".format(key, missing))

##Using indexes to speed up queries
###Projected field passed first, filter argument second
index_model = [('category', 1), ('year', -1)]
db.prizes.create_index(index_model)

###Finding disciplines that have not shared the prize in recent years
report = ''
for category in sorted(db.prizes.distinct('category')):
    doc = db.prizes.find_one(
        {'category': category, 'laureates.share': '1'},
        sort = [('year', -1)]
    )
    report += "{category}: {year}\n".format(**doc)

print(report)

###Counting laureates who were born and affiliated in the same country
db.laureates.create_index([('bornCountry', 1)])
n_born_and_affiliated = {country: db.laureates.count_documents({
    'bornCountry': country,
    'prizes.affiliations.country': country})
for country in db.laureates.distinct('bornCountry')}

Counter(n_born_and_affiliated).most_common(5)

##Limits
###Simple limit
list(db.prizes.find({"category": "economics"}, {"year": 1, "_id": 0}).sort("year").limit(3))

###Limit with simple share
filter_ = {'laureates.share': '4'}
projection = {'category': 1, 'year': 1, 'laureates.motivation': 1, '_id': 0}
cursor = db.prizes.find(filter_, projection).sort('year').limit(2)
pprint(list(cursor))

###Defining a function to return 3 entries per page
def get_part_laureates(page_number = 1, page_size = 3):
    if page_number < 1 or not isinstance(page_number, int):
        raise ValueError("Pages must start from 1")
    particle_laureates = list(db.laureates.find(
        {'prizes.motivation': {'$regex': 'particle'}},
        {'firstname': 1, 'surname': 1, 'prizes': 1, '_id': 0}).sort([('prizes.year', 1), ('surname', 1)])
    .skip(page_size * (page_number - 1)).limit(page_size))
    return particle_laureates

###Running a list comprehension to return 3 pages of data
[get_part_laureates(page_number = page) for page in range(1, 3)]

list(db.laureates.find(
    projection = {"firstname": 1, "prizes.year": 1, "_id": 0},
    filter = {"gender": "org"}).limit(3).sort("prizes.year", -1))

##Aggregations: server side filtering
###Building a simple pipeline for filtering
pipe = [{'$match': {'gender': {'$ne': 'org'}}},
        {'$project': {'bornCountry': 1, 'prizes.affiliations.country': 1}},
        {'$limit': 3}]

###List comprehension
for doc in db.laureates.aggregate(pipe):
    print("{bornCountry}: {prizes}".format(**doc))

###Pipelining to find years where one of the original categories was missing
pipeline = [{'$match': {'category': {'$in': sorted(orig_cats)}}},
            {'$project': {'year': 1, 'category': 1}},
            {'$sort': OrderedDict([('year', -1)])}]

cursor = db.prizes.aggregate(pipeline)

###Iterating over the cursor, using groupby to group by year
for key, group in groupby(cursor, key = itemgetter('year')):
    missing = orig_cats - {doc['category'] for doc in group}
    if missing:
        print('{year}: {missing}'.format(year = key, missing = ', '.join(sorted(missing))))

##Pathways
###Counting number of prizes awarded to organisations, using pathwyas instead of count docs
pipeline = [{'$match': {'gender': 'org'}},
            {'$project': {'n_prizes': {'$size': '$prizes'}}},
            {'$group': {'_id': None, 'n_prizes_total': {'$sum': '$n_prizes'}}}]

list(db.laureates.aggregate(pipeline))

###Long pipeline to highlight years where original categories were missing
orig_cats = sorted(set(db.prizes.distinct('category', {'year': '1901'})))
pipeline = [
    {"$match": {"category": {"$in": orig_cats}}},
    {"$project": {"category": 1, "year": 1}},

    # Collecting categories for each prize year.
    {"$group": {"_id": '$year', "categories": {"$addToSet": "$category"}}},

    # Projecting categories missing cats from a given year
    {"$project": {"missing": {"$setDifference": [orig_cats, "$categories"]}}},

    # Years with at least one missing category
    {"$match": {"missing.0": {"$exists": True}}},

    # Sort in reverse chronological order
    {"$sort": OrderedDict([("_id", -1)])},
]

for doc in db.prizes.aggregate(pipeline):
    print("{year}: {missing}".format(year=doc["_id"],missing=", ".join(sorted(doc["missing"]))))

###Counting the number of laureates by category
list(db.prizes.aggregate([
    {'$project': {'n_laureates': {'$size': '$laureates'}, 'category': 1}},
    {'$group': {'_id': '$category', 'n_laureates': {'$sum': '$n_laureates'}}},
    {'$sort': {'n_laureates': -1}}
]))

###Using unwind to count individual laureates and list IDs by year
list(db.prizes.aggregate([
        {'$unwind': '$laureates'},
        {'$project': {'year': 1, 'category': 1, 'laureates.id': 1}},
        {'$group': {'_id': {'$concat': ['$category', ':', '$year']},
            'laureate_ids': {'$addToSet': '$laureates.id'}}},
        {'$limit': 5}
]))

###Counting laureates winning prizes while affiliated with an institution in their birth country
key_ac = "prizes.affiliations.country"
key_bc = "bornCountry"

pipeline = [
    {"$project": {key_bc: 1, key_ac: 1}},

    # Ensure a single prize affiliation country per pipeline document
    {'$unwind': "$prizes"},
    {'$unwind': "$prizes.affiliations"},

    # Ensure values in the list of distinct values (so not empty)
    {"$match": {key_ac: {'$in': db.laureates.distinct(key_ac)}}},
    {"$project": {"affilCountrySameAsBorn": {
        "$gte": [{"$indexOfBytes": ["$"+key_ac, "$"+key_bc]}, 0]}}},

    # Count by "$affilCountrySameAsBorn" value (True or False)
    {"$group": {"_id": "$affilCountrySameAsBorn",
                "count": {"$sum": 1}}},
]
for doc in db.laureates.aggregate(pipeline): print(doc)
