import pymysql
import pygraphviz as pgv

# 数据库连接配置
host_name = '139.159.182.45'
db_name = 'fkc'
db_user = 'root'
db_password = '123456'

# 建立数据库连接
def get_db_connection():
    return pymysql.connect(host=host_name, port=3399, user=db_user, password=db_password, db=db_name, charset='utf8mb4')

# 递归查询所有子节点数据
def fetch_all_tree_data(cursor, tree_data, node_id=None):
    if node_id is not None:
        sql = "SELECT * FROM BinaryTree WHERE node_id = %s"
        cursor.execute(sql, (node_id,))
    else:
        # 如果没有指定node_id，就查询所有节点
        sql = "SELECT * FROM BinaryTree"
        cursor.execute(sql)

    results = cursor.fetchall()
    for result in results:
        # 检查节点是否已经存在于tree_data中
        if result['node_id'] not in tree_data:
            tree_data[result['node_id']] = result
            if result['left_child']:
                fetch_all_tree_data(cursor, tree_data, result['left_child'])
            if result['right_child']:
                fetch_all_tree_data(cursor, tree_data, result['right_child'])

# 递归绘制二叉树
def draw_tree(graph, tree_data, node_id):
    if node_id not in tree_data:
        return

    node = tree_data[node_id]
    #node_label = f"{node['node_name']}({node['node_id']})\nLeft Score: {node['left_score']}\nRight Score: {node['right_score']}"

    node_label = f"{node['node_name']}({node['node_id']})\nStore: {node['total_stores']}\nL: {node['left_score']}\nR: {node['right_score']}"

    graph.add_node(node_id, label=node_label)

    if node['left_child']:
        graph.add_node(node['left_child'], label=f"{tree_data[node['left_child']]['node_name']}({tree_data[node['left_child']]['node_id']})")
        graph.add_edge(node_id, node['left_child'], label='L')
        draw_tree(graph, tree_data, node['left_child'])

    if node['right_child']:
        graph.add_node(node['right_child'], label=f"{tree_data[node['right_child']]['node_name']}({tree_data[node['right_child']]['node_id']})")
        graph.add_edge(node_id, node['right_child'], label='R')
        draw_tree(graph, tree_data, node['right_child'])

# 绘制二叉树的主函数
def main():
    # 创建Graphviz图对象
    graph = pgv.AGraph(directed=True, strict=True)
    # 设置字体属性，确保图形、节点和边使用中文字体
    graph.graph_attr.update({'fontname': 'WenQuanYi Micro Hei'})
    graph.node_attr.update({'fontname': 'WenQuanYi Micro Hei'})
    graph.edge_attr.update({'fontname': 'WenQuanYi Micro Hei'})

    # 获取数据库连接
    conn = get_db_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        tree_data = {}
        # 查询所有节点信息
        fetch_all_tree_data(cursor, tree_data)
        # 假设您知道根节点的ID，这里使用您的根节点ID
        root_node_id = '3267811.1'
        draw_tree(graph, tree_data, root_node_id)
    finally:
        conn.close()
    
    # 渲染图形
    graph.layout(prog='dot')
    #graph.draw('binary_tree.png', format='png', encoding='UTF-8')
    graph.draw('binary_tree.png')

# 运行主函数
if __name__ == '__main__':
    main()

