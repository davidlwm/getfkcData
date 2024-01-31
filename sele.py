
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager



def get_cookies_from_fkcn(username, password):
    print("end")
    # 指定ChromeDriver的路径
    #chrome_driver_path = 'H:/美的代码/midea/go/c++/trunk/efficiency_promote/simulation_click/revesivation/chromedriver.exe'

    # 创建WebDriver实例
    #driver = webdriver.Chrome(executable_path=chrome_driver_path)

    # 使用 webdriver-manager 来自动管理驱动程序版本
    service = Service(ChromeDriverManager().install())

    # 创建 WebDriver 实例
    driver = webdriver.Chrome(service=service)

    #driver = webdriver.Chrome()
    driver.get('https://www.fkcn.com/index.xhtml')
    print("end")
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
    cookies = {cookie['name']: cookie['value'] for cookie in cookies}

    print(cookies)
    # 关闭WebDriver
    driver.quit()

    return cookies