import re
from lxml import html
import requests

from localMQ import *
from dao import *
from sele import *

import random
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#rootsId = '2410799'
rootsId = '2428971'
username = '3403001'
password = '123456'
host_name= '139.159.182.45'
db_name = 'fkc'
db_user = 'root'
db_password = '123456'
cookies = ''

# 树的结构用字典表示，每个节点对应其父节点的编号及其作为左（L）或右（R）子节点的信息
tree_structure = {
    # node: (parent, 'L/R')
    1: (0, 'L'),
    2: (0, 'R'),
    3: (1, 'L'),
    4: (1, 'R'),
    5: (2, 'L'),
    6: (2, 'R'),
    7: (3, 'L'),
    8: (3, 'R'),
    9: (4, 'L'),
    10: (4, 'R'),
    11: (5, 'L'),
    12: (5, 'R'),
    13: (6, 'L'),
    14: (6, 'R')
}

def add_parent_info(data, tree_structure):
    position_to_node = {d['position_id']: d for d in data}
    for d in data:
        parent_position = tree_structure.get(d['position_id'], (None,))[0]
        d['parent_node_id'] = position_to_node[parent_position]['node_id'] if parent_position in position_to_node else None
    return data

def add_children_info(data, tree_structure):
    position_to_node = {d['position_id']: d for d in data}
    for d in data:
        children = [node for node, (parent, _) in tree_structure.items() if parent == d['position_id']]
        left_child = next((position_to_node[child]['node_id'] for child in children if tree_structure[child][1] == 'L' and child in position_to_node), None)
        right_child = next((position_to_node[child]['node_id'] for child in children if tree_structure[child][1] == 'R' and child in position_to_node), None)

        d['left_child_node_id'] = left_child
        d['right_child_node_id'] = right_child

    return data


def add_tasks_from_data(data, conn):
    """ 从数据中提取特定任务并添加到 SQLite 队列 """
    for item in data:
        if 7 <= item['position_id'] <= 14:
            enqueue_task(conn, item['sId'])



def add_node_info(data, tree_structure):
    # 首先补充父节点信息
    data_with_parents = add_parent_info(data, tree_structure)

    # 接着补充子节点信息
    data_complete = add_children_info(data_with_parents, tree_structure)

    return data_complete


# def get_cookie(username, password):
#     url = 'https://www.fkcn.com/members/j_security_check'
#     headers = {
#         # 你的 headers
#     }
#     data = {
#         'j_username': username,
#         'j_password': password
#     }
#
#     session = requests.Session()
#     response = session.post(url, headers=headers, data=data)
#
#     if response.status_code == 200 :#and "登录成功的标志" in response.text:
#         print("Login successful.")
#         print(session.cookies.get_dict())
#         return session.cookies.get_dict()
#     else:
#         print("Login failed.")
#         return None


def fetch_store_info(cookies, params):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Referer': 'https://www.fkcn.com/members/office_home.xhtml',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    response = requests.get(
        'https://www.fkcn.com/members/storeInfo/storeNetworks.xhtml',
        params=params,
        cookies=cookies,
        headers=headers,
    )
    #print(response.text)

    return response.text



def check_for_login_state(html_content):
    tree = html.fromstring(html_content)
    # 寻找所有的<input>标签并检查'value'属性是否为'Login'
    login_buttons = tree.xpath("//input[@type='submit' and @class='buttons' and @value='Login']")
    if not login_buttons:
        return True  # 不存在登录按钮
    else:
        return False  # 存在登录按钮

def extract_store_data(html_content):
    # 初始化字典用于存储每个 STORE 的内容
    stores_data = {}

    # 使用正则表达式找到所有的 "STORE x BEGIN" 和 "STORE x END" 包围的节点
    store_regex = r'<!--\s*STORE\s*(\d+)\s*BEGIN\s*-->(.*?)<!--\s*STORE\s*\1\s*END\s*-->'
    store_matches = re.findall(store_regex, html_content, re.DOTALL)

    for store_num, store_html in store_matches:
        stores_data[int(store_num)] = store_html

    extracted_data = []

    # 处理每个 STORE 的数据
    for key, value in stores_data.items():
        store_html = html.fromstring(value)

        # 初始化结构体
        store_info = {
            'position_id': key,
            'sId': None,
            'name': None,
            'node_id': None,
            'date': None,
            'left_score': None,
            'right_score': None
        }

        # 提取数据
        try:
            # 提取 sId
            sId_link = store_html.xpath(".//a/@href")
            store_info['sId'] = sId_link[0].split('=')[-1].strip() if sId_link else '未找到'

            # 提取名称
            store_info['name'] = store_html.xpath(".//a/text()")[0].strip() if store_html.xpath(".//a/text()") else '未找到'

            # 定位 <br> 标签
            br_elements = store_html.xpath(".//br")

            # 第一个 <br> 标签后面是节点 ID 和日期
            if len(br_elements) > 0:
                br_tail = br_elements[0].tail.strip()
                node_id_date_match = re.search(r'(\d+\.\d+)\s+(\d{2}/\d{2}/\d{4})', br_tail)
                if node_id_date_match:
                    store_info['node_id'] = node_id_date_match.group(1)
                    store_info['date'] = node_id_date_match.group(2)

            # 第二个 <br> 标签后面是左右积分
            if len(br_elements) > 1:
                br_tail = br_elements[1].tail.strip()
                left_score_match = re.search(r'左积分:\s*(\d+)', br_tail)
                right_score_match = re.search(r'右积分:\s*(\d+)', br_tail)
                if left_score_match:
                    store_info['left_score'] = left_score_match.group(1)
                if right_score_match:
                    store_info['right_score'] = right_score_match.group(1)

            if store_info['node_id'] and store_info['node_id'] != '未找到':  # 检查 'node_id' 是否为空
                extracted_data.append(store_info)

        except Exception as e:
            print(f"STORE {key} 解析错误: {e}")

    print(extracted_data)
    return extracted_data

def process_tasks_from_queue(connection,conn, cookies):
    """ 从队列中处理任务 """
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

        #response = fetch_store_info(cookies, params)
        # if not check_for_login_state(response):
        #     get_cookies_from_fkcn(username, password)
        #     time.sleep(5)
        #     response = fetch_store_info(cookies, params)


        with open('total.html', 'r', encoding='utf-8') as file:
            response = file.read()

        data = extract_store_data(response)

        add_tasks_from_data(data, conn)
        data = add_node_info(data, tree_structure)

        insert_or_update_data(connection, data)

        # 完成任务后从队列中删除
        complete_task(conn, task_id)
        random_delay = random.uniform(5, 15)
        time.sleep(random_delay)

def main():
    logging.info("------------------------------process begin------------------------------\n\n")
    #cookies = get_cookies_from_fkcn(username, password);

    connection = create_db_connection(host_name, db_name, db_user, db_password)
    db_conn = create_connection('task_queue.db')
    create_table(db_conn)
    reset_processing_tasks(db_conn)
    count = count_pending_tasks(db_conn)
    if count ==0:
        enqueue_task(db_conn, rootsId)

    process_tasks_from_queue(connection,db_conn, cookies)
    logging.info("------------------------------process end------------------------------\n\n")

    db_conn.close()

if __name__ == "__main__":
    main()