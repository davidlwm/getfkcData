import mysql.connector
from mysql.connector import Error
import logging

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
        logging.error(f"The error '{e}' occurred")

    return connection

def create_binary_tree_table(conn):
    """ Create the BinaryTree table if it doesn't exist """
    try:
        sql_create_binary_tree_table = """ CREATE TABLE IF NOT EXISTS BinaryTree (
                                        primary_id INT NOT NULL AUTO_INCREMENT,
                                        node_id VARCHAR(16) NOT NULL,
                                        left_child VARCHAR(16) DEFAULT NULL,
                                        right_child VARCHAR(16) DEFAULT NULL,
                                        parent_node VARCHAR(16) DEFAULT NULL,
                                        node_name VARCHAR(255) DEFAULT NULL,
                                        left_score INT DEFAULT NULL,
                                        right_score INT DEFAULT NULL,
                                        timestamp VARCHAR(32) DEFAULT NULL,
                                        depth INT DEFAULT NULL,
                                        remarks TEXT,
                                        sId VARCHAR(8) DEFAULT NULL,
                                        last_update_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                        PRIMARY KEY (primary_id),
                                        UNIQUE KEY node_id (node_id)
                                    ); """

        cursor = conn.cursor()
        cursor.execute(sql_create_binary_tree_table)
    except Error as e:
        logging.error(f"The error '{e}' occurred")


def insert_or_update_data(connection, data):
    sql = """
    INSERT INTO BinaryTree (node_id, left_child, right_child, parent_node, node_name, left_score, right_score, timestamp, sId, last_update_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CONVERT_TZ(NOW(), 'UTC', '+08:00'))
    ON DUPLICATE KEY UPDATE
    left_child = VALUES(left_child),
    right_child = VALUES(right_child),
    parent_node = VALUES(parent_node),
    node_name = VALUES(node_name),
    left_score = VALUES(left_score),
    right_score = VALUES(right_score),
    timestamp = VALUES(timestamp),
    sId = VALUES(sId),
    last_update_time = CONVERT_TZ(NOW(), 'UTC', '+08:00');
    """

    # Prepare the data to insert or update
    values = []
    for d in data:
        values.append((
            d['node_id'],
            d.get('left_child_node_id'),
            d.get('right_child_node_id'),
            d.get('parent_node_id'),
            d['name'],
            int(d['left_score']),
            int(d['right_score']),
            d['date'],
            d['sId']
        ))

    cursor = connection.cursor()
    try:
        # Use executemany to insert or update multiple rows at once
        cursor.executemany(sql, values)
        connection.commit()
        logging.info("Data inserted or updated successfully")
    except Error as e:
        logging.error(f"The error '{e}' occurred")
    finally:
        cursor.close()

