# database_utils.py
import mysql.connector

def connect_to_mysql(host, port, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            auth_plugin='mysql_native_password'
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def execute_query(connection, query):
    try:
        print(connection)
        cursor = connection.cursor()

        cursor.execute(query)

        # Lấy kết quả
        result = cursor.fetchall()

        return result
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    finally:
        # Không đóng cursor ở đây
        pass

