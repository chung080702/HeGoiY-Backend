# app.py
from flask import Flask, jsonify, request
import traceback
from database import connect_to_mysql, execute_query
from query import Parameter, master_compiler
from flask_cors import CORS

host = 'localhost' 
port = 3306  
user = 'my_user'
password = 'my_password'
database = 'my_database'

app = Flask(__name__)
cors = CORS(app, resources={"/api/*": {"origin": "http://localhost:3000"}})

def to_bed_object(str: str):
    res={}
    arr = str.split(',')
    for item in arr:
        [name,count] = item.split('-')
        res[name] = int(count)
    return res

@app.route('/api/v1/test', methods = ['POST'])
def test():
    try:
        p = {
            'request_room_service': ','.join(map(str, [11,6,8,10])),
            'request_room_facility': ','.join(map(str,[2686,203,204,209,30,222,141,230,109,44,25,145,85,2679,143,2650,223,227,231,2652,49,217,275,147,41,40,86,2666,29,276,2653,99,228,2665,2685,232,210,213,33,87,2687,278,280,279,2383,34,101,221,2656,2644,277]))
        }
        
        res = execute_query(connection, f""" 
                    SELECT * from 
                    (   SELECT * 
                        from rooms r
                        LIMIT 5
                    ) r 
                    LEFT JOIN
                    (   SELECT r.id, GROUP_CONCAT(rf.name) as service_names  
                        FROM rooms r, room_feature_relations rfr, room_features rf  
                        WHERE r.id = rfr.room_id AND rf.id = rfr.feature_id AND rfr.feature_id in ({p["request_room_service"]}) 
                        GROUP BY r.id
                    ) s
                    ON r.id = s.id
                    LEFT JOIN 
                    (   SELECT r.id, GROUP_CONCAT(rf.name) as facilities_names 
                        FROM rooms r, room_facility_relations rfr, room_facilities rf  
                        WHERE r.id = rfr.room_id AND rf.id = rfr.facility_id AND rfr.facility_id in ({p['request_room_facility']}) 
                        GROUP BY r.id
                    ) f
                    ON r.id = f.id
                    LEFT JOIN 
                    (   SELECT r.id, GROUP_CONCAT(CONCAT(b.name, '-', rbr.count)) as facilities_names 
                        FROM rooms r, room_bed_relations rbr, beds b  
                        WHERE r.id = rbr.room_id AND b.id = rbr.bed_id
                        GROUP BY r.id
                    ) b
                    ON r.id = b.id
                    """)
        
        rooms = []
        if res:
            rooms = [
                        {
                            "id": row[0], 
                            "before_discount_price": row[1],
                            "cheapest_price": row[2],
                            "image_url": row[4],
                            "name": row[5],
                            "view": row[6],
                            "services": row[8].split(',') if row[7] else  None,
                            "facilities": row[10].split(',') if row[8] else  None,
                            "beds": to_bed_object(row[12]) if row[9] else None
                        } 
                        for row in res
                    ]
        return jsonify({"rooms": rooms})
    except:
        return jsonify({'error': 'Invalid JSON data'}), 400
      

@app.route('/api/v1/query', methods = ['POST'])
def index():  
    queries = None
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
        p.request_longitude = str(longitude)
        p.request_latitude = str(latitude)
        p.request_hotel_feature = ','.join(map(str, hotelServices)) if len(hotelServices) > 0 else 'NULL'
        p.request_star = str(star)

        p.processSpecial()
        query = master_compiler.compile(p)
        queries = query.split(';')

        execute_query(connection, queries[0])
        execute_query(connection, queries[1])
        execute_query(connection, queries[2])
        execute_query(connection, queries[3])
        result = execute_query(connection, queries[4])
       
        hotels = []
        if result:
            hotels = [{
                        "id": row[0], 
                        "name": row[1], 
                        "address": row[2], 
                        "star": row[3], 
                        "roomIds": row[4].split(","),
                        "hotelServices": row[5].split(',') if len(row[5])>0 else None
                       } 
                       for row in result]

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
                                "id": row[0] ,
                                "view": row[3] ,
                                "beds": {
                                    "single_bed": row[4],
                                    "king_bed":row[7],
                                    "double_bed":row[8],
                                    "queen_bed":row[9],
                                    "super_king_bed":row[12],
                                    "semi_double_bed":row[13],
                                    "sofa_bed":row[14] ,
                                    "bunk_bed":row[15] ,
                                    "japanese_futon":row[16],
                                },
                                "cheapest_price":row[5],
                                "before_discount_price":row[6] ,
                                "services": row[18].split(',') if row[18] else  None,
                                "facilities": row[20].split(',') if row[20] else None
                            }
                        
                        hotel["rooms"].append(room)
                  
                   
            return jsonify({"hotels": hotels})
        else:
            return jsonify({"message":  "No hotel"})
    except:
        print(traceback.format_exc())
        return jsonify({'error': 'Invalid JSON data'}), 400  # Bad Request
    finally:
        if queries != None:
            execute_query(connection, queries[5])
            execute_query(connection, queries[6])
            execute_query(connection, queries[7])

# Lấy 5 hotel đầu tiên
@app.route('/api/v1/metadata')
def get_metadata():  
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
        

        bedTypes = []
        bedTypesQuery = execute_query(connection,"SELECT * FROM beds")
        if bedTypesQuery:
            bedTypes = [{"id": row[0], "name": row[1]} for row in bedTypesQuery]


        roomViews = []
        roomViewsQuery = execute_query(connection, "SELECT DISTINCT view FROM rooms WHERE view IS NOT NULL AND view != ''")
        if roomViewsQuery:
            roomViews = [row[0] for row in roomViewsQuery]

        return jsonify({'roomServices':roomServices, 'hotelServices': hotelServices, 'bedTypes':bedTypes,'roomViews':roomViews,'roomFacilities': roomFacilities})
    except:
        return jsonify({'error': 'Server Error'}), 400  # Bad Request

if __name__ == '__main__':
    connection = connect_to_mysql(host, port, user, password, database)
    
    app.run(debug=True, port=8000)
    
    connection.close()
