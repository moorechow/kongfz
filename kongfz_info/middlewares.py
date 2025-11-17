# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pickle
import os
import logging

class KongfzInfoSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # maching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class KongfzInfoDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class KongfzLoginMiddleware:
    """孔夫子登录中间件"""

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.cookies_file = 'kongfz_cookies.pkl'
        self.driver = None
        self.logger = self.setup_logger()

    def setup_logger(self):
        """设置logger"""
        logger = logging.getLogger('KongfzLoginMiddleware')
        if not logger.handlers:
            # 避免重复添加handler
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            username=crawler.settings.get('KONGFZ_USERNAME'),
            password=crawler.settings.get('KONGFZ_PASSWORD')
        )

    def get_driver(self):
        """获取浏览器驱动"""
        options = webdriver.ChromeOptions()
        # options.add_argument('--headless')  # 无头模式
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        # 反自动化检测
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # 移除自动化特征
        options.add_argument('--disable-blink-features')
        options.add_argument('--disable-features=VizDisplayCompositor')

        # 用户代理
        options.add_argument('--window-size=1920,1080')
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        # 语言设置
        options.add_argument('--lang=zh-CN')  # 设置语言
        options.add_argument('--accept-lang=zh-CN,zh;q=0.9,en;q=0.8')  # 接受语言

        # 性能优化（孔夫子网站图片较多）
        options.add_argument('--disable-images')  # 提高爬取速

        # 禁用不必要的功能
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-notifications')

        return webdriver.Chrome(options=options)

    def load_cookies(self):
        """加载已保存的cookies"""
        if os.path.exists(self.cookies_file):
            with open(self.cookies_file, 'rb') as f:
                return pickle.load(f)
        return None

    def save_cookies(self, cookies):
        """保存cookies"""
        with open(self.cookies_file, 'wb') as f:
            pickle.dump(cookies, f)

    def login_with_selenium(self):
        """使用Selenium模拟登录（直接访问登录页面）"""
        login_url = 'https://login.kongfz.com/Pc/Login/iframe?returnUrl=https://www.kongfz.com/'
        self.driver = self.get_driver()

        try:
            # 直接访问登录页面
            self.driver.get(login_url)
            self.logger.info(f"已访问登录页面: {login_url}")

            # 等待页面加载完成
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            self.logger.info("登录页面加载完成")

            # 检查是否需要处理iframe
            iframe_elements = self.driver.find_elements(By.TAG_NAME, "iframe")
            if iframe_elements:
                self.logger.info("检测到iframe，尝试切换到iframe")
                self.driver.switch_to.frame(iframe_elements[0])

            # 等待登录表单元素加载
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "loginName"))
            )
            self.logger.info("登录表单已加载")

            # 输入用户名
            username_input = self.driver.find_element(By.ID, 'loginName')
            username_input.clear()
            username_input.send_keys(self.username)
            self.logger.info("已输入用户名")

            # 输入密码
            password_input = self.driver.find_element(By.ID, 'password')
            password_input.clear()
            password_input.send_keys(self.password)
            self.logger.info("已输入密码")

            # 查找并点击登录按钮
            login_button = self.find_login_button()
            if login_button:
                # 先滚动到按钮可见
                self.driver.execute_script("arguments[0].scrollIntoView();", login_button)
                time.sleep(1)

                # 尝试多种点击方式
                try:
                    login_button.click()
                except:
                    # 如果普通点击失败，使用JavaScript点击
                    self.driver.execute_script("arguments[0].click();", login_button)

                self.logger.info("已点击登录按钮")
            else:
                self.logger.error("找不到登录按钮")
                return None

            # 等待登录完成 - 检查是否跳转到首页
            success = self.wait_for_login_success()

            if success:
                # 获取cookies
                cookies = self.driver.get_cookies()
                self.save_cookies(cookies)
                self.logger.info(f"登录成功！获取到 {len(cookies)} 个cookies")

                # 验证登录是否真正成功
                if self.verify_login_status():
                    self.logger.info("登录状态验证成功")
                    return cookies
                else:
                    self.logger.warning("登录状态验证失败")
                    return None
            else:
                self.logger.error("登录失败：未成功跳转到首页")
                return None

        except Exception as e:
            self.logger.error(f"登录过程出错: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
        finally:
            if self.driver:
                self.driver.quit()
                self.logger.info("浏览器已关闭")


    def switch_to_login_iframe(self):
        """切换到登录框的iframe（如果需要）"""
        try:
            # 查找登录相关的iframe
            iframe_selectors = [
                "iframe[src*='login']",
                "iframe[id*='login']",
                "iframe[class*='login']",
                "#loginBox iframe",
            ]

            for selector in iframe_selectors:
                try:
                    iframe = self.driver.find_element(By.CSS_SELECTOR, selector)
                    self.driver.switch_to.frame(iframe)
                    self.logger.info("切换到登录iframe")
                    return True
                except Exception:
                    continue
        except Exception as e:
            self.logger.warning(f"切换iframe失败或不需要切换: {e}")

        return False

    def find_login_button(self):
        """查找登录按钮"""
        button_selectors = [
            "input.login_submit",  # 类选择器
            "input.btn_red_h40",  # 另一个类选择器
            "input[type='submit']",  # 属性选择器
            "input[value*='登录']",  # 部分匹配value属性
        ]

        for selector in button_selectors:
            try:
                button = self.driver.find_element(By.CSS_SELECTOR, selector)
                if button.is_displayed() and button.is_enabled():
                    self.logger.info(f"找到登录按钮: {selector}")
                    return button
            except Exception as e:
                self.logger.debug(f"选择器 {selector} 失败: {str(e)}")
                continue

        # 如果CSS选择器都失败，尝试XPath
        xpath_selectors = [
            "//input[@value=' 登 录']",  # 精确匹配value
            "//input[contains(@value, '登录')]",  # 包含匹配
        ]

        for xpath in xpath_selectors:
            try:
                button = self.driver.find_element(By.XPATH, xpath)
                if button.is_displayed() and button.is_enabled():
                    self.logger.info(f"找到登录按钮(XPath): {xpath}")
                    return button
            except Exception as e:
                self.logger.debug(f"XPath选择器 {xpath} 失败: {str(e)}")
                continue

        self.logger.error("所有登录按钮选择器都失败了")
        return None

    def wait_for_login_success(self, timeout=30):
        """等待登录成功"""
        try:
            # 等待条件：登录框消失或用户信息出现
            WebDriverWait(self.driver, timeout).until(
                lambda driver: (
                    # 登录框消失
                        len(driver.find_elements(By.ID, "loginBox")) == 0 or
                        # 或者用户信息出现
                        len(driver.find_elements(By.CLASS_NAME, "user-info")) > 0 or
                        len(driver.find_elements(By.CLASS_NAME, "user-name")) > 0 or
                        # 检查URL变化（如果有）
                        "login" not in driver.current_url
                )
            )
            return True
        except Exception as e:
            self.logger.error(f"等待登录成功超时: {e}")
            return False

    def get_valid_cookies(self):
        """获取有效的cookies"""
        # 先尝试加载已保存的cookies
        cookies = self.load_cookies()
        if cookies and self.verify_cookies(cookies):
            return cookies

        # 如果cookies无效，重新登录
        return self.login_with_selenium()

    def verify_cookies(self, cookies):
        """验证cookies是否有效"""
        # 这里可以添加验证逻辑，比如访问一个需要登录的页面
        # 简化处理，暂时认为cookies有效
        return True

    def verify_login_status(self):
        """验证登录状态"""
        try:
            # 检查是否已登录：查找用户名的元素
            logged_in_indicators = [
                "#nickName .info-text",  # 用户名的选择器
                ".user-name",  # 其他可能的用户信息选择器
                ".user-info",
                "a[href*='user']",  # 用户相关链接
            ]

            # 检查未登录状态：查找"登录/注册"文本
            not_logged_indicators = [
                "#nickName .info-text:contains('登录')",
                "#nickName .info-text:contains('注册')",
                "a:contains('登录')",
                "a:contains('注册')",
            ]

            # 首先检查是否显示未登录状态
            for selector in not_logged_indicators:
                try:
                    if selector.endswith(":contains('登录')") or selector.endswith(":contains('注册')"):
                        # 使用XPath处理包含文本的选择器
                        text = "登录" if "登录" in selector else "注册"
                        elements = self.driver.find_elements(By.XPATH, f"//*[contains(text(), '{text}')]")
                        if elements:
                            self.logger.error(f"登录失败：找到未登录指示器 '{text}'")
                            return False
                    else:
                        # 普通CSS选择器
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            self.logger.error(f"登录失败：找到未登录指示器 '{selector}'")
                            return False
                except Exception:
                    continue

            # 然后检查是否显示已登录状态
            for selector in logged_in_indicators:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        # 检查元素文本是否不是"登录"或"注册"
                        for element in elements:
                            text = element.text.strip()
                            if text and text not in ["登录", "注册"]:
                                self.logger.info(f"登录成功：找到用户信息 '{text}'")
                                return True
                except Exception:
                    continue

            # 最后检查URL是否跳转到首页（作为辅助判断）
            if self.driver.current_url == "https://www.kongfz.com/":
                self.logger.info("登录成功：已跳转到首页")
                return True

            self.logger.warning("登录状态验证：未找到明确的用户标识")
            return False  # 改为返回False，因为无法确认登录状态

        except Exception as e:
            self.logger.error(f"验证登录状态时出错: {str(e)}")
            return False