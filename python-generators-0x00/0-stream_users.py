#!/usr/bin/python3
import mysql.connector
from mysql.connector import Error

DB_NAME = "ALX_prodev"

def stream_users():
    """
    Generator function that streams rows from user_data table one by one.
    Yields dicts with keys: user_id, name, email, age.
    """
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="AlbrightHuman",       
            password="423497",
            database=DB_NAME
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT user_id, name, email, age FROM user_data")

        # ✅ only one loop as required
        for row in cursor:
            yield row

        cursor.close()
        connection.close()

    except Error as e:
        print(f"Error streaming data: {e}")
        return
