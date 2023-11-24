# app.py
from flask import Flask, jsonify
from database import connect_to_mysql, execute_query

host = 'localhost' 
port = 3306  
user = 'my_user'
password = 'my_password'
database = 'my_database'

app = Flask(__name__)

# Lấy 5 hotel đầu tiên
@app.route('/api/v1')
def index():  
    query = "SELECT * FROM hotels LIMIT 5"
    result = execute_query(connection, query)

    if result:
        hotels = [{"id": row[0], "star_rating": row[1], "address": row[2]} for row in result]
        return jsonify({"hotels": hotels})
    else:
        return jsonify({"message": "No hotels found."})
    

if __name__ == '__main__':
    connection = connect_to_mysql(host, port, user, password, database)

    app.run(debug=True, port=8000)
    
    connection.close()
