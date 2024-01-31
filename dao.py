import mysql.connector
from mysql.connector import Error

def create_db_connection(host_name, db_name, db_user, db_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            database=db_name,
            user=db_user,
            password=db_password
        )
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


def insert_or_update_data(connection, data):
    sql = """
    INSERT INTO BinaryTree (node_id, left_child, right_child, parent_node, node_name, left_score, right_score, timestamp, sId)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    left_child = VALUES(left_child),
    right_child = VALUES(right_child),
    parent_node = VALUES(parent_node),
    node_name = VALUES(node_name),
    left_score = VALUES(left_score),
    right_score = VALUES(right_score),
    timestamp = VALUES(timestamp),
    sId = VALUES(sId);
    """

    # 准备要插入或更新的数据
    values = []
    for d in data:
        values.append((
            d['node_id'],
            d.get('left_child_node_id'),
            d.get('right_child_node_id'),
            d.get('parent_node_id'),
            d['name'],
            int(d['left_score']),  # 假设 left_score 和 right_score 是整数
            int(d['right_score']),
            d['date'],
            d['sId']
        ))

    cursor = connection.cursor()
    try:
        # 使用 execumany 一次性插入或更新多行数据
        cursor.executemany(sql, values)
        connection.commit()
        print("Data inserted or updated successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

