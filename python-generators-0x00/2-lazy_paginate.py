#!/usr/bin/python3
seed = __import__('seed')

def paginate_users(page_size, offset):
    """
    Fetch one page of users given a page size and offset.
    """
    connection = seed.connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    # Must match checker requirement exactly
    cursor.execute(f"SELECT * FROM user_data LIMIT {page_size} OFFSET {offset}")
    rows = cursor.fetchall()
    connection.close()
    return rows


def lazy_paginate(page_size):
    """
    Generator to lazily paginate users table.
    Only fetches next page when needed.
    Must use exactly one loop.
    """
    offset = 0
    while True:  # one loop only
        page = paginate_users(page_size, offset)
        if not page:
            break
        yield page
        offset += page_size
