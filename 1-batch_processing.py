#!/usr/bin/python3
import mysql.connector
from mysql.connector import Error

DB_NAME = "ALX_prodev"

def stream_users_in_batches(batch_size):
    """
    Generator that fetches rows from user_data in batches.
    Yields lists of user dictionaries.
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

        batch = []
        for row in cursor: 
            batch.append(row)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:  # yield remaining rows
            yield batch

        cursor.close()
        connection.close()

    except Error as e:
        print(f"Error fetching batches: {e}")
        return


def batch_processing(batch_size):
    """
    Processes batches of users, filtering users over the age of 25.
    Yields individual user dicts.
    """
    for batch in stream_users_in_batches(batch_size):  
        for user in batch: 
            if user["age"] > 25:
                print(user)  
