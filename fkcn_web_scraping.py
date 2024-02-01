
from store_data_extraction import *
from task_queue_management import *
from data_operations import *
from fkcn_login import *

import random
import logging
import os

# Define the path to the log file in the current directory
log_file_path = os.path.join(os.getcwd(), 'my_log.log')

# Configure logging to write to the specified log file
logging.basicConfig(level=logging.INFO, filename=log_file_path, format='%(asctime)s - %(levelname)s - %(message)s')

rootsId = '737980'
#rootsId = '2428971'
#username = '3403001'
username = '2420187'
password = '888999'
host_name = '139.159.182.45'
db_name = 'fkc'
db_user = 'root'
db_password = '123456'
taskCount = 0


def process_store_info(params, cookies, conn):
    #global username, password, tree_structure

    response = fetch_store_info(cookies, params)
    if not check_for_login_state(response):
        cookies = get_cookies_from_fkcn(username, password)
        time.sleep(5)
        response = fetch_store_info(cookies, params)

    # with open('total.html', 'r', encoding='utf-8') as file:
    #     response = file.read()
    data = extract_store_data(response)

    add_tasks_from_data(data, conn)
    data = add_node_info(data, tree_structure)

    insert_or_update_data(conn, data)

def process_tasks_from_queue(conn, cookies):
    """ 从队列中处理任务 """
    global taskCount
    while True:
        # 从队列中获取任务
        task = dequeue_task(conn)
        if task is None:
            logging.info("No more tasks in the queue.")
            break

        task_id, sId = task
        logging.info('task: %s', task)
        # 执行任务
        params = {'sId': sId}

        process_store_info(params, cookies, conn)
        # 完成任务后从队列中删除
        complete_task(conn, task_id)
        taskCount = taskCount + 1
        if taskCount >= 20:
            time.sleep(random.uniform(60, 100))
            taskCount = 0
        time.sleep(random.uniform(25, 40))

def main():
    logging.info("------------------------------process begin------------------------------\n\n")
    print("start")
    cookies = load_cookies(path_to_cookies)
    if not cookies:
        cookies = get_cookies_from_fkcn(username, password)
    print(cookies)

    conn = create_db_connection(host_name, db_name, db_user, db_password)
    create_binary_tree_table(conn)
    create_tasks_table(conn)
    reset_processing_tasks(conn)
    count = count_pending_tasks(conn)
    if count == 0:
        enqueue_task(conn, rootsId)

    process_tasks_from_queue(conn, cookies)
    conn.close()
    logging.info("------------------------------process end------------------------------\n\n")

if __name__ == "__main__":
    main()