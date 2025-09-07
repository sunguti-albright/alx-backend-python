#!/usr/bin/python3
import mysql.connector
from mysql.connector import Error
import uuid
import csv

DB_NAME = "ALX_prodev"

def connect_db():
    """Connects to the MySQL database server"""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="AlbrightHuman",        
            password="423497" 
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def create_database(connection):
    """Creates the database ALX_prodev if it does not exist"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        cursor.close()
    except Error as e:
        print(f"Error creating database: {e}")


def connect_to_prodev():
    """Connects to the ALX_prodev database in MySQL"""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="AlbrightHuman",        
            database=DB_NAME
        )
        return connection
    except Error as e:
        print(f"Error connecting to ALX_prodev: {e}")
        return None


def create_table(connection):
    """Creates a table user_data if it does not exist"""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_data (
                user_id CHAR(36) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                age DECIMAL NOT NULL
            )
        """)
        connection.commit()
        cursor.close()
        print("Table user_data created successfully")
    except Error as e:
        print(f"Error creating table: {e}")


def insert_data(connection, csv_file):
    """Inserts data from CSV into user_data table if it does not exist"""
    try:
        cursor = connection.cursor()
        with open(csv_file, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) != 3:
                    continue
                name, email, age = row
                user_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO user_data (user_id, name, email, age)
                    VALUES (%s, %s, %s, %s)
                """, (user_id, name, email, age))
        connection.commit()
        cursor.close()
    except Error as e:
        print(f"Error inserting data: {e}")


def stream_users(connection):
    """
    Generator function that streams rows from user_data table one by one
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT user_id, name, email, age FROM user_data")
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            yield row
        cursor.close()
    except Error as e:
        print(f"Error streaming data: {e}")
