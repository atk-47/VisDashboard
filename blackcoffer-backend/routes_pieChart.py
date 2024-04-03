from pymongo import MongoClient, ASCENDING
from flask import jsonify


# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Blackcoffer']  # database name
collection = db['BlackCoffer'] #collection name

def sampleData():
    data = {
        "A":20,
        "B":25,
        "c":30,
        "D":25,
    }

    return jsonify(data), 200

#@app.route('/topics-count', methods=['GET'])
def get_topics_count():
    pipeline = [
        {
            '$group': {
                '_id': '$topic',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'count': -1}  # Optional: Sort by count in descending order
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    
    # Convert results to a dictionary for better JSON serialization
    topics_count = {item['_id']: item['count'] for item in results}
    topics_count.pop("")#removing the empty topic count

    #aggregating all the countries with count less than 10 as others
    others = 0
    for item in topics_count.copy():
        if(topics_count[item] < 10):
            others += topics_count[item]
            topics_count.pop(item)

    topics_count["others"] = others
    
    return jsonify(topics_count), 200

#@app.route('/country-count', methods=['GET'])
def get_country_count():
    pipeline = [
        {
            '$group': {
                '_id': '$country',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'count': -1}  # Optional: Sort by count in descending order
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    
    # Convert results to a dictionary for better JSON serialization
    country_count = {item['_id']: item['count'] for item in results}
    country_count.pop("")#removing the empty topic count

    #aggregating all the countries with count less than 10 as others
    others = 0
    for item in country_count.copy():
        if(country_count[item] < 10):
            others += country_count[item]
            country_count.pop(item)

    country_count["others"] = others
    country_count["USA"] = country_count["United States of America"]
    country_count.pop("United States of America")
    
    return jsonify(country_count), 200

#@app.route('/sector-count', methods=['GET'])
def get_sector_count():
    pipeline = [
        {
            '$group': {
                '_id': '$sector',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'count': -1}  # Optional: Sort by count in descending order
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    
    # Convert results to a dictionary for better JSON serialization
    sector_count = {item['_id']: item['count'] for item in results}
    sector_count.pop("")#removing the empty topic count

    #aggregating all the countries with count less than 10 as others
    others = 0
    for item in sector_count.copy():
        if(sector_count[item] < 10):
            others += sector_count[item]
            sector_count.pop(item)

    sector_count["others"] = others
    
    return jsonify(sector_count), 200