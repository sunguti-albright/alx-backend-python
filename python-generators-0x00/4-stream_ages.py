#!/usr/bin/python3
seed = __import__('seed')

def stream_user_ages():
    """
    Generator to stream user ages one by one.
    """
    connection = seed.connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT age FROM user_data")

    for row in cursor:   
        yield row["age"]

    connection.close()


def average_user_age():
    """
    Computes the average age using the generator without loading all into memory.
    """
    total, count = 0, 0
    for age in stream_user_ages():   
        total += age
        count += 1

    avg = total / count if count > 0 else 0
    print(f"Average age of users: {avg:.2f}")
    return avg


if __name__ == "__main__":
    average_user_age()
