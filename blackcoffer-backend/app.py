from pymongo import MongoClient, ASCENDING
from flask import Flask, jsonify
from flask_cors import CORS
import routes_pieChart, routes_Others


# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Blackcoffer']  # database name
collection = db['BlackCoffer'] #collection name

app = Flask(__name__)
CORS(app)


@app.route('/')
def home():
    return 'base'

@app.route('/api/data', methods=['GET'])
def get_data():
    data = []
    for document in collection.find():  # Retrieve all documents from the collection
        data.append({
            'id': str(document['_id']),  # Convert ObjectId to string
            'end_year': document['end_year'],  
            'intensity': document['intensity'],   
            'sector': document['sector'],
            'topic': document['topic'],
            'insight': document['insight'],
            'url': document['url'],
            'region': document['region'],
            'start_year': document['start_year'],
            'impact': document['impact'],
            'added': document['added'],
            'published': document['published'],
            'country': document['country'],
            'relevance': document['relevance'],
            'pestle': document['pestle'],
            'source': document['source'],
            'title': document['title'],
            'likelihood': document['likelihood'],
        })
    return jsonify(data), 200

#heatmap Routes
app.add_url_rule('/intensity-by-country', view_func=routes_Others.intensityByCountry)
app.add_url_rule('/likelihood-by-country', view_func=routes_Others.likelihoodByCountry)
app.add_url_rule('/relevance-by-country', view_func=routes_Others.relevanceByCountry)

#adding PieChart Routes
app.add_url_rule('/sample-data', view_func=routes_pieChart.sampleData)
app.add_url_rule('/topics-count', view_func=routes_pieChart.get_topics_count)
app.add_url_rule('/country-count', view_func=routes_pieChart.get_country_count)
app.add_url_rule('/sector-count', view_func=routes_pieChart.get_sector_count)




if __name__ == '__main__':
    app.run(debug=True)
