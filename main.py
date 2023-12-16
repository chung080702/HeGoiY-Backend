# app.py
from flask import Flask, jsonify, request
from database import connect_to_mysql, execute_query
from query import SQLCompiler, Parameter

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
        star = data["star"]

        s = SQLCompiler()
        p = Parameter() 
        
        p.request_view = str(roomView)
        p.request_double_bed = str(beds.get("double_bed",'NULL'))
        p.request_single_bed = str(beds.get("single_bed",'NULL'))
        p.request_sofa_bed = str(beds.get("sofa_bed",'NULL'))
        p.request_king_bed = str(beds.get("king_bed",'NULL'))
        p.request_queen_bed = str(beds.get("queen_bed",'NULL'))
        p.request_super_king_bed =  str(beds.get("super_king_bed",'NULL'))
        p.request_semi_double_bed =  str(beds.get("semi_double_bed",'NULL'))
        p.request_bunk_bed = str(beds.get("bunk_bed",'NULL'))
        p.request_japanese =  str(beds.get("japanese_futon",'NULL'))
        p.request_price_low = priceStart
        p.request_price_high = priceEnd
        p.request_room_facility = ','.join(map(str, roomFacilities)) if len(roomFacilities) > 0 else 'NULL'
        p.request_room_service =  ','.join(map(str, roomServices)) if len(roomServices) > 0 else 'NULL'
        p.request_longtitude = str(longitude)
        p.request_latitude = str(latitude)
        p.request_hotel_feature = ','.join(map(str, hotelServices)) if len(hotelServices) > 0 else 'NULL'
        p.request_star = str(star)

        p.processSpecial()
        query = s.compile(p)
        queries = query.split(';')

        execute_query(connection, queries[0])
        execute_query(connection, queries[1])
        execute_query(connection, queries[2])
        execute_query(connection, queries[3])
        result = execute_query(connection, queries[4])
        
        hotels = []
        if result:
            hotels = [{"id": row[0], "name": row[1], "address": row[2], "star": row[3], "roomIds": row[4].split(","),"hotelServices": row[5].split(",")} for row in result]
            for hotel in hotels:
                hotel["rooms"] = []
                for roomId in hotel["roomIds"]:
                    
                    roomResult = execute_query(connection, f""" 
                    SELECT * from 
                    (   SELECT * 
                        from rooms r
                        WHERE r.id = {roomId}
                    ) r 
                    LEFT JOIN
                    (   SELECT r.id, GROUP_CONCAT(rf.name) as service_names  
                        FROM rooms r, room_feature_relations rfr, room_features rf  
                        WHERE r.id = {roomId} && r.id = rfr.room_id AND rf.id = rfr.feature_id AND rfr.feature_id in ({p.request_room_service}) 
                        GROUP BY r.id
                    ) s
                    ON r.id = s.id
                    LEFT JOIN 
                    (   SELECT r.id, GROUP_CONCAT(rf.name) as facilities_names 
                        FROM rooms r, room_facility_relations rfr, room_facilities rf  
                        WHERE r.id = {roomId} && r.id = rfr.room_id AND rf.id = rfr.facility_id AND rfr.facility_id in ({p.request_room_facility}) 
                        GROUP BY r.id
                    ) f
                    ON r.id = f.id""")
                    
                    if roomResult:
                        row = roomResult[0]
                        
                        room =  {
                                "id": row[0] or 'NULL',
                                "view": row[3] or 'NULL',
                                "beds": {
                                    "single_bed": row[4] or 'NULL',
                                    "king_bed":row[7] or 'NULL',
                                    "double_bed":row[8] or 'NULL',
                                    "queen_bed":row[9] or 'NULL',
                                    "super_king_bed":row[12] or 'NULL',
                                    "semi_double_bed":row[13]or 'NULL',
                                    "sofa_bed":row[14] or 'NULL',
                                    "bunk_bed":row[15] or 'NULL',
                                    "japanese_futon":row[16] or 'NULL',
                                },
                                "cheapest_price":row[5] or 'NULL',
                                "before_discount_price":row[6] or 'NULL',
                                "services": row[18].split(',') if row[18] else  'NULL',
                                "facilities": row[20].split(',') if row[20] else 'NULL'
                            }
                        
                        hotel["rooms"].append(room)
                  
                    
                    
            return jsonify({"hotels": hotels})
        else:
            return jsonify({"message":  "No hotel"})
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
        

        bedTypes = ["single_bed","double_bed","sofa_bed","king_bed","queen_bed","super_king_bed","semi_double_bed","bunk_bed","japanese_futon"]

        roomViews = []
        roomViewsQuery = execute_query(connection, "SELECT DISTINCT view FROM rooms")
        if roomViewsQuery:
            roomViews = [row[0] for row in roomViewsQuery]

        return jsonify({'roomServices':roomServices, 'hotelServices': hotelServices, 'bedTypes':bedTypes,'roomViews':roomViews,'roomFacilities': roomFacilities})
    except:
        return jsonify({'error': 'Server Error'}), 400  # Bad Request

if __name__ == '__main__':
    connection = connect_to_mysql(host, port, user, password, database)
    
    app.run(debug=True, port=8000)
    
    connection.close()
