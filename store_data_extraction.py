import re
from lxml import etree
from lxml import html
import requests
from task_queue_management import *

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
    """ 从数据中提取特定任务并添加到队列 """
    for item in data:
        if 7 <= item['position_id'] <= 14:
            enqueue_task(conn, item['sId'])

def add_node_info(data, tree_structure):
    # 首先补充父节点信息
    data_with_parents = add_parent_info(data, tree_structure)

    # 接着补充子节点信息
    data_complete = add_children_info(data_with_parents, tree_structure)

    return data_complete

def fetch_store_info(cookies, params):
    print(cookies)

    # Extract name and value
    cookie_name = list(cookies.keys())[0]
    cookie_value = cookies[cookie_name]

    cookies = {
        cookie_name: cookie_value,
    }
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
    #加try

    response = requests.get(
        'https://www.fkcn.com/members/storeInfo/storeNetworks.xhtml',
        params=params,
        cookies=cookies,
        headers=headers,
    )
    return response.text

def check_for_login_state(html_content):
    html_content = html_content.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
    tree = html.fromstring(html_content)
    # 寻找所有的<input>标签并检查'value'属性是否为'Login'
    login_buttons = tree.xpath("//input[@type='submit' and @class='buttons' and @value='Login']")
    if not login_buttons:
        return True  # 不存在登录按钮
    else:
        return False  # 存在登录按钮

def extract_store_info(store_html, position_id):
    position_id = int(position_id)
    # 初始化存储信息的字典
    store_info = {
        'position_id': position_id,
        'sId': None,
        'name': None,
        'node_id': None,
        'date': None,
        'total_stores':None,
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

        # 处理节点ID和日期
        if len(br_elements) > 0:
            br_tail = br_elements[0].tail.strip()
            node_id_date_match = re.search(r'(\d+\.\d+)\s+(\d{2}/\d{2}/\d{4})', br_tail)
            if node_id_date_match:
                store_info['node_id'] = node_id_date_match.group(1)
                store_info['date'] = node_id_date_match.group(2)

        # 处理左右积分
        if len(br_elements) > 1:
            br_tail = br_elements[1].tail.strip()
            left_score_match = re.search(r'左积分:\s*(\d+)', br_tail)
            right_score_match = re.search(r'右积分:\s*(\d+)', br_tail)
            if left_score_match:
                store_info['left_score'] = left_score_match.group(1)
            if right_score_match:
                store_info['right_score'] = right_score_match.group(1)

    except Exception as e:
        print(f"STORE {position_id} 解析错误: {e}")

    return store_info

def extract_9to12stores_from_html(html_content):
    document = html.fromstring(html_content)
    stores_info = []
    position_id = 9  # 初始position_id

    # 查找所有的td标签
    store_td_elements = document.xpath("//td")
    for store_td in store_td_elements:
        # 检查td标签内是否有实际内容（例如，是否包含<div>标签）
        if store_td.find(".//div[@class='activeStore']") is not None:
            # 将找到的td标签转换为字符串
            store_html_str = html.tostring(store_td, encoding='unicode', pretty_print=True)
            store_html_str = html.fromstring(store_html_str)
            # 提取商店信息
            store_info = extract_store_info(store_html_str, position_id)
            stores_info.append(store_info)

            position_id += 1  # 为下一个有实际信息的td元素更新position_id

    return stores_info

def extract_store_data(html_content):
    # 使用lxml解析整个HTML内容
    html_bytes = html_content.encode('utf-8')
    tree = html.fromstring(html_bytes)

    # 使用XPath查找所有匹配的元素
    elements = tree.xpath("//td//strong")
    total_stores = ''
    # 提取第三个匹配元素的文本内容
    if len(elements) >= 3:
        total_stores = elements[2].text_content()
    # 初始化字典用于存储每个 STORE 的内容
    stores_data = {}

    stores_data9to2list = []
    # 正则表达式查找 <!-- STORE 9 BEGIN --> 与 <!-- STORE 12 END --> 之间的内容
    pattern = r"<!--\s*STORE\s*9\s*BEGIN\s*-->(.*?)<!--\s*STORE\s*12\s*END\s*-->"
    match = re.search(pattern, html_content, re.DOTALL)

    if match:
        content_between = match.group(1)
        stores_data9to2list =  extract_9to12stores_from_html(content_between)

    # 使用正则表达式找到所有的 "STORE x BEGIN" 和 "STORE x END" 包围的节点
    store_regex = r'<!--\s*STORE\s*(\d+)\s*BEGIN\s*-->(.*?)<!--\s*STORE\s*\1\s*END\s*-->'
    store_matches = re.findall(store_regex, html_content, re.DOTALL)

    for store_num, store_html in store_matches:
        stores_data[int(store_num)] = store_html

    extracted_data = []

    # 处理每个 STORE 的数据
    for key, value in stores_data.items():
        store_html = html.fromstring(value)
        store_info = extract_store_info(store_html, key)
        if key == 0:
            store_info['total_stores'] = int(total_stores)
        if store_info['node_id'] and store_info['node_id'] != '未找到':  # 检查 'node_id' 是否为空
            extracted_data.append(store_info)

    extracted_data.extend(stores_data9to2list)

    extracted_data.sort(key=lambda x: x['position_id'])

    print(extracted_data)
    return extracted_data

