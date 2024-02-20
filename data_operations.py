import mysql.connector
from mysql.connector import Error
import logging
import sys

def create_db_connection(host_name,port, db_name, db_user, db_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            port=port,
            database=db_name,
            user=db_user,
            password=db_password
        )
    except Error as e:
        logging.error(f"The error '{e}' occurred")
        sys.exit("Database connection failed")  # 退出程序

    return connection


def fetch_root_nodes_sId(conn):
    # SQL 查询，只选择没有父节点的节点的 sId
    query = "SELECT sId FROM BinaryTree WHERE primary_id <> 1 and parent_node IS NULL;"

    try:
        # 创建一个游标对象，用于执行 SQL 语句
        with conn.cursor() as cursor:
            # 执行 SQL 查询
            cursor.execute(query)
            # 获取所有查询结果
            results = cursor.fetchall()
            # 将结果转换为单个列表，假设你只想要 sId 列表
            sId_list = [row[0] for row in results] if results else []
            return sId_list
    except Exception as e:
        print(f"Error fetching root nodes' sId: {e}")
        return []

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
                                        date VARCHAR(32) DEFAULT NULL,
                                        depth INT DEFAULT NULL,
                                        remarks TEXT,
                                        sId VARCHAR(8) DEFAULT NULL,
                                        total_stores int DEFAULT NULL,
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
    INSERT INTO BinaryTree (node_id, left_child, right_child, parent_node, node_name, left_score, right_score, date, sId, last_update_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CONVERT_TZ(NOW(), 'UTC', '+08:00'))
    ON DUPLICATE KEY UPDATE
    left_child = VALUES(left_child),
    right_child = VALUES(right_child),
    parent_node = VALUES(parent_node),
    node_name = VALUES(node_name),
    left_score = VALUES(left_score),
    right_score = VALUES(right_score),
    date = VALUES(date),
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

def update_data_single(connection, data):
    # 检查记录是否存在
    check_sql = "SELECT COUNT(*) FROM BinaryTree WHERE node_id = %s;"
    check_values = (data['node_id'],)

    cursor = connection.cursor()
    cursor.execute(check_sql, check_values)
    record_count = cursor.fetchone()[0]

    if record_count == 0:
        # 如果记录不存在，执行插入操作
        insert_sql = """
        INSERT INTO BinaryTree (
            node_id,
            left_child,
            right_child,
            parent_node,
            node_name,
            left_score,
            right_score,
            date,
            sId,
            total_stores,
            last_update_time
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CONVERT_TZ(NOW(), 'UTC', '+08:00')
        );
        """
        insert_values = (
            data['node_id'],
            data.get('left_child_node_id'),
            data.get('right_child_node_id'),
            data.get('parent_node_id'),
            data['name'],
            int(data['left_score']),
            int(data['right_score']),
            data['date'],
            data['sId'],
            data['total_stores']
        )

        try:
            cursor.execute(insert_sql, insert_values)
            connection.commit()
            logging.info("New record inserted for node_id {}".format(data['node_id']))
        except Error as e:
            logging.error(f"The error '{e}' occurred during insertion")
    else:
        # 如果记录存在，执行更新操作
        update_sql = """
        UPDATE BinaryTree
        SET
            left_child = %s,
            right_child = %s,
            total_stores = %s,
            last_update_time = CONVERT_TZ(NOW(), 'UTC', '+08:00')
        WHERE node_id = %s;
        """
        update_values = (
            data.get('left_child_node_id'),
            data.get('right_child_node_id'),
            data.get('total_stores'),
            data['node_id']
        )

        try:
            #print(update_sql, update_values)
            cursor.execute(update_sql, update_values)
            connection.commit()
            logging.info("Data updated successfully for node_id {}".format(data['node_id']))
        except Error as e:
            logging.error(f"The error '{e}' occurred during update")
        finally:
            cursor.close()

def update_parent_nodes(connection, data):
    # SQL 更新语句模板
    update_sql = """
        UPDATE BinaryTree
        SET
            parent_node = %s
        WHERE node_id = %s;
    """

    # 准备更新数据
    values = [(d.get('parent_node_id', ''), d['node_id']) for d in data]

    cursor = connection.cursor()
    try:
        # 使用executemany来一次性更新多行
        #print(update_sql, values)
        cursor.executemany(update_sql, values)
        connection.commit()
        logging.info("Parent nodes updated successfully")
    except Error as e:
        # 如果发生错误则回滚
        connection.rollback()
        logging.error(f"The error '{e}' occurred")
    finally:
        cursor.close()