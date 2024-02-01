
import time
import json
import os
path_to_cookies = 'cookies.json'

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def save_cookies(cookies, path):
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
    # 确保文件存在，如果不存在则创建
    open(path, 'a').close()
    # 清空文件内容
    with open(path, 'w') as file:
        # 保存新的cookies字典
        json.dump(cookies_dict, file)

def load_cookies(path):
    # 检查文件是否存在且不为空
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, 'r') as file:
            cookies_dict = json.load(file)
        return cookies_dict
    else:
        # 如果文件不存在或为空，返回空字典
        return {}

def get_cookies_from_fkcn(username, password):
    # 使用 webdriver-manager 来自动管理驱动程序版本
    service = Service(ChromeDriverManager().install())

    # 创建 WebDriver 实例
    driver = webdriver.Chrome(service=service)

    #driver = webdriver.Chrome()
    driver.get('https://www.fkcn.com/index.xhtml')

    # 等待元素加载
    wait = WebDriverWait(driver, 10)
    china_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[.//img[contains(@src, 'zh_flag.gif')]]")))

    # 点击该链接
    china_link.click()

    # 等待元素加载
    wait = WebDriverWait(driver, 10)
    login_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[span[contains(text(), '登录')]]")))

    # 点击该链接
    login_link.click()

    time.sleep(5)

    # 等待登录页面的用户名和密码输入框加载完毕
    username_input = wait.until(EC.presence_of_element_located((By.ID, 'username')))
    password_input = wait.until(EC.presence_of_element_located((By.ID, 'pwd')))

    # 输入用户名和密码
    username_input.send_keys(username)
    password_input.send_keys(password)

    # 点击登录按钮
    login_button = driver.find_element(By.XPATH, "//input[@value='登录']")
    login_button.click()

    # 等待一段时间以确保登录请求完成
    time.sleep(5)

    # 获取cookies
    cookies = driver.get_cookies()
    save_cookies(cookies, path_to_cookies)
    print(cookies)
    # 关闭WebDriver
    driver.quit()

    return cookies