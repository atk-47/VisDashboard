from pymongo import MongoClient, ASCENDING
from flask import jsonify


# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Blackcoffer']  # database name
collection = db['BlackCoffer'] #collection name

#@app.route('/intensity-by-country', methods=['GET'])
def intensityByCountry():
    #Aggregation pipeline to group by a column (e.g., 'country') and sum another column (e.g., 'intensity')
    pipeline = [
        {
            "$group":
            {
                "_id": "$country",  # Group by the 'country' field
                "intensity_sum": { "$sum" : "$intensity"}  # Sum the 'intensity' field for each country
            }
        },
    ]
    data_intensity = list(collection.aggregate(pipeline))
    result = {}
    #to convert the data format to support GeoJSON and topoJSON
    for item in data_intensity:
        result[item["_id"]] = item["intensity_sum"]

    result.pop("")

    others = 0
    for item in result.copy():
        if(result[item] < 70):
            others += result[item]
            result.pop(item)

    result["others"] = others
    result["USA"] = result["United States of America"]
    result.pop("United States of America")
    
    return jsonify(result), 200

#@app.route('/likelihood-by-country', methods=['GET'])
def likelihoodByCountry():
    #Aggregation pipeline to group by a column (e.g., 'country') and sum another column (e.g., 'likelihood')
    pipeline = [
        {
            "$group":
            {
                "_id": "$country",  # Group by the 'country' field
                "likelihood_sum": { "$sum" : "$likelihood"}  # Sum the 'likelihood' field for each country
            }
        },
    ]
    data_likelihood = list(collection.aggregate(pipeline))
    result = {}
    #to convert the data format to support GeoJSON and topoJSON
    for item in data_likelihood:
        result[item["_id"]] = item["likelihood_sum"] 

    others = 0
    for item in result.copy():
        if(result[item] < 20):
            others += result[item]
            result.pop(item)
    result.pop("")

    result["others"] = others
    result["USA"] = result["United States of America"]
    result.pop("United States of America")
    

    return jsonify(result), 200

#@app.route('/relevance-by-country', methods=['GET'])
def relevanceByCountry():
    #Aggregation pipeline to group by a column (e.g., 'country') and sum another column (e.g., 'relevance')
    pipeline = [
        {
            "$group":
            {
                "_id": "$country",  # Group by the 'country' field
                "relevance_sum": { "$sum" : "$relevance"}  # Sum the 'relevance' field for each country
            }
        },
    ]
    data_relevance = list(collection.aggregate(pipeline)) 
    result = {}
    #to convert the data format to support GeoJSON and topoJSON
    for item in data_relevance:
        result[item["_id"]] = item["relevance_sum"]

    result.pop("")
    others = 0
    for item in result.copy():
        if(result[item] < 20):
            others += result[item]
            result.pop(item)

    result["others"] = others
    result["USA"] = result["United States of America"]
    result.pop("United States of America")

    return jsonify(result), 200