import sqlite3
from sqlite3 import Error
import logging
# 初始化和连接到 SQLite 数据库
def create_connection(db_file):
    """ 创建一个数据库连接到 SQLite 指定数据库 """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

# 创建队列表
def create_table(conn):
    """ 创建一个表，如果不存在的话，并为task列添加唯一索引 """
    try:
        sql_create_tasks_table = """ CREATE TABLE IF NOT EXISTS tasks (
                                        id INTEGER PRIMARY KEY,
                                        task TEXT NOT NULL UNIQUE,
                                        status TEXT NOT NULL
                                    ); """
        cursor = conn.cursor()
        cursor.execute(sql_create_tasks_table)
    except Error as e:
        print(e)

# 向队列中添加任务
def task_exists(cursor, task):
    """ 检查任务是否已经存在于数据库中 """
    cursor.execute("SELECT 1 FROM tasks WHERE task=?", (task,))
    return cursor.fetchone() is not None


def enqueue_task(conn, task):
    """ 将新任务添加到队列中，如果任务已存在则不添加 """
    logging.info("enqueue_task begin")

    cursor = conn.cursor()

    # 检查任务是否已存在
    if task_exists(cursor, task):
        logging.info(f"Task '{task}' already exists, not enqueued.")
        return None  # 或者可以返回现有任务的 ID

    # 插入新任务
    try:
        sql_insert_task = ''' INSERT INTO tasks(task, status) VALUES(?,?) '''
        cursor.execute(sql_insert_task, (task, 'pending'))
        conn.commit()
        logging.info("enqueue_task end")
        return cursor.lastrowid
    except sqlite3.IntegrityError as e:
        logging.error(f"Failed to enqueue task '{task}'. Error: {e}")
        return None

# 获取任务
def dequeue_task(conn):
    """ 获取任务并将其状态标记为 'processing' """
    logging.info("dequeue_task begin")
    sql_get_task = ''' SELECT id, task FROM tasks
                       WHERE status = 'pending'
                       ORDER BY id ASC
                       LIMIT 1 '''
    cursor = conn.cursor()
    cursor.execute(sql_get_task)
    task = cursor.fetchone()
    if task:
        sql_update_status = ''' UPDATE tasks
                                SET status = 'processing'
                                WHERE id = ? '''
        cursor.execute(sql_update_status, (task[0],))
        conn.commit()
    logging.info("dequeue_task end")
    return task

# 完成任务后从队列中删除
def complete_task(conn, task_id):
    """ 完成任务并从队列中删除 """
    logging.info("complete_task begin")
    sql_delete_task = ''' DELETE FROM tasks WHERE id = ? '''
    cursor = conn.cursor()
    cursor.execute(sql_delete_task, (task_id,))
    conn.commit()
    logging.info("complete_task end")

def reset_processing_tasks(conn):
    """ 将所有 'Processing' 状态的任务重置为 'Pending' """
    sql_reset_task = ''' UPDATE tasks SET status = 'pending' WHERE status = 'processing' '''
    cursor = conn.cursor()
    cursor.execute(sql_reset_task)
    conn.commit()
    cursor.close()

def count_pending_tasks(conn):
    """ 计算状态为 'Pending' 的任务数量 """
    sql_count_pending_tasks = ''' SELECT COUNT(*) FROM tasks WHERE status = 'pending' '''
    cursor = conn.cursor()
    cursor.execute(sql_count_pending_tasks)
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def demo():
    # 使用示例
    db_conn = create_connection('task_queue.db')
    create_table(db_conn)

    # 添加任务到队列
    task_id = enqueue_task(db_conn, 'Task 1')

    # 从队列获取任务
    task = dequeue_task(db_conn)
    if task:
        print(f"Processing task: {task[1]}")
        # 假设任务处理完毕，完成任务
        complete_task(db_conn, task[0])

    # 关闭数据库连接
    db_conn.close()







