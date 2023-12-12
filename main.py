# app.py
from flask import Flask, jsonify, request
from database import connect_to_mysql, execute_query

host = 'localhost' 
port = 3306  
user = 'my_user'
password = 'my_password'
database = 'my_database'

app = Flask(__name__)

# Lấy 5 hotel đầu tiên
@app.route('/api/v1/query')
def index():  
    try:
        data = request.json
        latitude = data["latitude"]
        longitude = data["longitude"]
        priceStart = data["priceStart"]
        priceEnd = data["priceEnd"]
        roomServices = data["roomServices"]
        roomFacilities = data["roomFacilities"]
        hotelServices = data["hotelServices"]
        roomView = data["roomView"]
        beds = data["beds"]
        score = data["score"]

        print(latitude)
        print(longitude)
        print(priceStart)
        print(priceEnd)
        print(roomServices)
        print(roomFacilities)
        print(hotelServices)
        print(roomView)
        print(beds)
        print(score)

        query = "SELECT * FROM hotels LIMIT 5"
        result = execute_query(connection, query)
        
        if result:
            hotels = [{"id": row[0], "star_rating": row[1], "address": row[2]} for row in result]
            return jsonify({"hotels": hotels})
        else:
            return jsonify({"message": "No hotels found."})
    except:
        return jsonify({'error': 'Invalid JSON data'}), 400  # Bad Request

# Lấy 5 hotel đầu tiên
@app.route('/api/v1/metadata')
def getMetadata():  
    try:
        roomServices = []
        roomServicesQuery = execute_query(connection, "SELECT * FROM room_features")
        if roomServicesQuery:
            roomServices = [{"id": row[0], "name": row[1]} for row in roomServicesQuery]
       
        roomFacilities = []
        roomFacilitiesQuery = execute_query(connection, "SELECT * FROM room_facilities")
        if roomFacilitiesQuery:
            roomFacilities = [{"id": row[0], "name": row[1]} for row in roomFacilitiesQuery]

        hotelServices = []
        hotelServicesQuery = execute_query(connection, "SELECT * FROM hotel_features")
        if hotelServicesQuery:
            hotelServices = [{"id": row[0], "name": row[1]} for row in hotelServicesQuery]
        

        bedTypes = ["single_bed","double_bed","sofa_bed","king_bed","king_bed","queen_bed","super_king_bed","semi_double_bed","bunk_bed","japanese_futon"]

        roomViews = []
        roomViewsQuery = execute_query(connection, "SELECT DISTINCT view FROM rooms")
        if roomViewsQuery:
            roomViews = [row[0] for row in roomViewsQuery]

        return jsonify({'roomServices':roomServices, 'hotelServices': hotelServices, 'bedTypes':bedTypes,'roomViews':roomViews,'roomFacilities': roomFacilities})
    except:
        return jsonify({'error': 'Server Error'}), 400  # Bad Request

if __name__ == '__main__':
    connection = connect_to_mysql(host, port, user, password, database)
    res = execute_query(connection,"")
    app.run(debug=True, port=8000)
    
    connection.close()
