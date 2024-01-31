
from dataHandle import *
from dao import *
from sele import *

import random
import logging
import os

# Define the path to the log file in the current directory
log_file_path = os.path.join(os.getcwd(), 'my_log.log')

# Configure logging to write to the specified log file
logging.basicConfig(level=logging.INFO, filename=log_file_path, format='%(asctime)s - %(levelname)s - %(message)s')

#rootsId = '2410799'
rootsId = '2428971'
username = '3403001'
password = '123456'
host_name= '139.159.182.45'
db_name = 'fkc'
db_user = 'root'
db_password = '123456'
connection = ''
cookies = ''

def process_store_info(params, cookies,connection):
    #global username, password, connection, tree_structure

    # response = fetch_store_info(cookies, params)
    # if not check_for_login_state(response):
    #     get_cookies_from_fkcn(username, password)
    #     time.sleep(5)
    #     response = fetch_store_info(cookies, params)

    with open('total.html', 'r', encoding='utf-8') as file:
        response = file.read()
    data = extract_store_data(response)

    add_tasks_from_data(data, connection)
    data = add_node_info(data, tree_structure)

    insert_or_update_data(connection, data)

def process_tasks_from_queue(connection, cookies):
    """ 从队列中处理任务 """
    while True:
        # 从队列中获取任务
        task = dequeue_task(connection)
        if task is None:
            logging.info("No more tasks in the queue.")
            break

        task_id, sId = task
        logging.info('task: %s', task)
        # 执行任务
        params = {'sId': sId}

        process_store_info(params, cookies,connection)
        # 完成任务后从队列中删除
        complete_task(connection, task_id)
        random_delay = random.uniform(5, 15)
        time.sleep(random_delay)

def main():
    logging.info("------------------------------process begin------------------------------\n\n")

    # cookies = get_cookies_from_fkcn(username, password);
    connection = create_db_connection(host_name, db_name, db_user, db_password)
    create_binary_tree_table(connection)
    create_tasks_table(connection)
    reset_processing_tasks(connection)
    count = count_pending_tasks(connection)
    if count ==0:
        enqueue_task(connection, rootsId)

    process_tasks_from_queue(connection, cookies)
    logging.info("------------------------------process end------------------------------\n\n")

    connection.close()

if __name__ == "__main__":
    main()