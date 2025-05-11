import pymysql
import scrtsxx
# Configuration for Server 1 (REMOTE)
server1_config = {
    'host': scrtsxx.RHOST,
    'user': scrtsxx.USERNAME,
    'password': scrtsxx.PASSWORD,
    'db': scrtsxx.DB
}

# Configuration for Server 2 (LOCAL)
server2_config = {
    'host': scrtsxx.HOST,
    'user': scrtsxx.USERNAME,
    'password': scrtsxx.PASSWORD,
    'db': scrtsxx.DB
}


conn1 = pymysql.connect(**server1_config)
conn2 = pymysql.connect(**server2_config)

try:
    with conn1.cursor() as cursor1:
        cursor1.execute("SELECT MAX(id) FROM ratings_user")
        max_id_server1 = cursor1.fetchone()[0] or 0

    with conn2.cursor() as cursor2:
        cursor2.execute("SELECT uuid, node_address, rating, timestamp FROM ratings_user2")
        rows = cursor2.fetchall()

    with conn1.cursor() as cursor1:
        insert_sql = """
            INSERT INTO ratings_user (uuid, node_address, rating, timestamp)
            VALUES (%s, %s, %s, %s)
        """
        cursor1.executemany(insert_sql, rows)
        conn1.commit()

    with conn2.cursor() as cursor2:
        cursor2.execute("DELETE FROM ratings_user")
        conn2.commit()

    with conn2.cursor() as cursor2:
        new_autoinc = max_id_server1 + 1
        cursor2.execute(f"ALTER TABLE ratings_user AUTO_INCREMENT = {new_autoinc}")
        conn2.commit()

    print("Data successfully transferred and auto-increment reset on Server 2.")

except Exception as e:
    print(f"An error occurred: {e}")
    conn1.rollback()
    conn2.rollback()

finally:
    conn1.close()
    conn2.close()