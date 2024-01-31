import logging
from mysql.connector import Error

# Create the tasks table
def create_tasks_table(conn):
    """ Create a table if it doesn't exist and add a unique index for the task column """
    try:
        sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                        id INT AUTO_INCREMENT PRIMARY KEY,
                                        task INT NOT NULL UNIQUE,
                                        status CHAR(16) NOT NULL
                                    );"""
        cursor = conn.cursor()
        cursor.execute(sql_create_tasks_table)
    except Error as e:
        print(e)

# Check if a task exists in the database
def task_exists(cursor, task):
    """ Check if a task already exists in the database """
    cursor.execute("SELECT 1 FROM tasks WHERE task=%s", (task,))
    return cursor.fetchone() is not None

# Enqueue a task into the queue
def enqueue_task(conn, task):
    """ Add a new task to the queue if it doesn't already exist """
    logging.info("enqueue_task begin")

    cursor = conn.cursor()

    # Check if the task already exists
    if task_exists(cursor, task):
        logging.info(f"Task '{task}' already exists, not enqueued.")
        return None  # Or you can return the ID of the existing task

    # Insert the new task
    try:
        sql_insert_task = ''' INSERT INTO tasks(task, status) VALUES(%s, %s) '''
        cursor.execute(sql_insert_task, (task, 'pending'))
        conn.commit()
        logging.info("enqueue_task end")
        return cursor.lastrowid
    except Error as e:
        logging.error(f"Failed to enqueue task '{task}'. Error: {e}")
        return None

# Dequeue a task from the queue
def dequeue_task(conn):
    """ Get a task and mark its status as 'processing' """
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
                                WHERE id = %s '''
        cursor.execute(sql_update_status, (task[0],))
        conn.commit()
    logging.info("dequeue_task end")
    return task

# Complete a task and remove it from the queue
def complete_task(conn, task_id):
    """ Update the task status to 'delete' """
    logging.info("complete_task begin")
    sql_update_task = ''' UPDATE tasks SET status = 'delete' WHERE id = %s '''
    cursor = conn.cursor()
    cursor.execute(sql_update_task, (task_id,))
    conn.commit()
    logging.info("complete_task end")

# Count the number of tasks with status 'Pending'
def count_pending_tasks(conn):
    """ Count the number of tasks with status 'Pending' """
    sql_count_pending_tasks = ''' SELECT COUNT(*) FROM tasks WHERE status = 'pending' '''
    cursor = conn.cursor()
    cursor.execute(sql_count_pending_tasks)
    count = cursor.fetchone()[0]
    cursor.close()
    return count

# Reset all tasks with status 'Processing' to 'Pending'
def reset_processing_tasks(conn):
    """ Reset all tasks with 'Processing' status to 'Pending' """
    sql_reset_task = ''' UPDATE tasks SET status = 'pending' WHERE status = 'processing' '''
    cursor = conn.cursor()
    cursor.execute(sql_reset_task)
    conn.commit()
    cursor.close()


def demo():
    pass