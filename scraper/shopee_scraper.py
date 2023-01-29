import json
import logging
import os
import time

from bs4 import BeautifulSoup
from selenium.common import TimeoutException, StaleElementReferenceException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from scraper.common_scraper import CommonScraper

logger = logging.getLogger(__name__)


class ShopeeScraper(CommonScraper):
    def __init__(self, num_page_to_scrape=10, data_dir='./data/shopee', wait_timeout=5, retry_num=3,
                 restart_num=20, is_headless=False):
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        main_page = "https://shopee.vn/"
        super().__init__(num_page_to_scrape, data_dir, wait_timeout, retry_num, restart_num, is_headless, main_page)

    def get_product_urls(self):
        for cat_1 in os.listdir(self.data_dir):
            full_cat_1 = os.path.join(self.data_dir, cat_1)
            for cat_2 in os.listdir(full_cat_1):
                full_cat_2 = os.path.join(full_cat_1, cat_2)
                with open(os.path.join(full_cat_2, 'base_url.txt')) as f:
                    d = json.load(f)
                category = d['category']
                category_url = d['url']

                logger.info("Scraping category: " + category)
                output_dir = os.path.join(self.data_dir, category)
                if not os.path.exists(output_dir):
                    os.mkdir(output_dir)
                self.driver.get(category_url)

                # iterate through each page and scrape products link
                counter = 1
                prev_url = ''
                while True:
                    logger.info("Scraping urls from page " + str(counter))
                    if prev_url == self.driver.current_url:
                        logger.info('This page is identical to the previous page, which means last page is reached')
                        break
                    prev_url = self.driver.current_url
                    for retry in range(self.retry_num):
                        try:
                            self.driver.execute_script("window.scrollTo(0,0)")
                            WebDriverWait(self.driver, self.wait_timeout).until(
                                ec.visibility_of_element_located((By.CLASS_NAME, "shopee-search-item-result__item")))
                            for i in range(12):
                                self.driver.execute_script("window.scrollBy(0,350)")
                                time.sleep(0.05)

                            soup = BeautifulSoup(self.driver.page_source, features="lxml")
                            product_list = soup.find_all(class_='shopee-search-item-result__item')
                            product_list_with_url = soup \
                                .find(class_='row shopee-search-item-result__items') \
                                .find_all('a', href=True)
                            num_product = len(product_list)
                            num_product_with_url = len(product_list_with_url)
                            logger.info(f'Found {num_product} products')
                            logger.info(f'Found {num_product_with_url} products with url')

                            if len(product_list_with_url) == len(product_list):
                                logger.info('All products\' urls are loaded, extracting product url')
                                self._get_product_urls(product_list, category)
                                break
                            elif retry == self.retry_num - 1:
                                logger.info(f'Only {num_product_with_url}/{num_product} product url are found '
                                            f'after retrying {self.retry_num} times')
                            else:
                                logger.info(f'{num_product_with_url}/{num_product} product url are found, retrying')

                        except TimeoutException:
                            logger.info(f'Products not visible after waiting for {self.wait_timeout}, retrying')

                    if counter == self.num_page_to_scrape:
                        logger.info(f'Reached maximum number of pages to scrape in category: {category}')
                        break
                    counter += 1

                    logger.info('Going to the next page')
                    next_page_button = self.driver.find_element(by=By.CLASS_NAME, value="shopee-icon-button--right")
                    next_page_button.click()

    def _get_product_urls(self, product_list, category):
        for product in product_list:
            product_url = 'https://shopee.vn' + product.a['href']
            self.write_to_file(product_url, os.path.join(category, 'url.txt'))

    def get_product_info(self):
        for cat_1 in os.listdir(self.data_dir):
            full_cat_1 = os.path.join(self.data_dir, cat_1)
            for cat_2 in os.listdir(full_cat_1):
                full_cat_2 = os.path.join(full_cat_1, cat_2)
                with open(os.path.join(full_cat_2, 'base_url.txt')) as f:
                    d = json.load(f)
                category = d['category']

                logger.info('Scraping category: ' + category)
                category_path = os.path.join(self.data_dir, category)
                url_path = os.path.join(category_path, 'url.txt')

                done = self.check_done_info(category_path)
                if done:  # skip this category if all url are scraped
                    logger.info(f"Category \"{category}\" is scraped already, skipping")
                    continue
                with open(url_path) as urls:
                    for i, url in enumerate(urls):
                        if i < self.get_curr_url_num(category_path):  # continue scraping from the last scraped url
                            logger.info(f"Url number \"{i}\" is scraped already, skipping")
                            continue

                        self.log_curr_url_num(category_path, i)
                        url = url.strip()
                        logger.info(f'Scraping url number {i}: {url}')
                        if i != 0 and i % self.restart_num == 0:
                            logger.info('Restart number reached, restarting driver')
                            self.restart_driver()
                        self.driver.get(url)

                        try:
                            # get all product types
                            type_arr = []
                            WebDriverWait(self.driver, self.wait_timeout).until(
                                ec.visibility_of_element_located((By.CLASS_NAME, 'flex.rY0UiC.j9be9C')))
                            type_ele = self.driver.find_element(By.CLASS_NAME, 'flex.rY0UiC.j9be9C')
                            logger.info('Product types found, iterating through each type...')
                            for retry in range(self.retry_num):
                                try:
                                    # grab all product type button into an array
                                    for counter, attr in enumerate(
                                            type_ele.find_elements(By.XPATH, './div/div[@class=\'flex items-center\']')):
                                        buttons = attr.find_elements(By.XPATH, './div/button[@class=\'product-variation\']')
                                        if len(buttons) == 0:
                                            logger.info(
                                                f'Attribute number {counter} has no clickable buttons, skipping this attribute')
                                            continue
                                        type_arr.append(buttons)
                                    logger.info(f'{len(type_arr)} types found, iterating through all of them')
                                    self._iterate_all_product_type(0, type_arr, url=url, category_path=category_path)
                                    break

                                except StaleElementReferenceException:
                                    logger.info('Cannot get product types, retrying')

                                if retry == self.retry_num - 1:
                                    logger.info(f'Cannot get product types after {self.retry_num} attempts')
                                    self._get_product_info_helper(url, category_path)
                        except TimeoutException:
                            logger.info('Product has no type, scraping directly')
                            self._get_product_info_helper(url, category_path)

                        except Exception as e:
                            logger.error(e)

                    self.log_done_info(category_path)

    def _iterate_all_product_type(self, type_index, type_arr, **kwargs):
        if type_index == len(type_arr):
            self._get_product_info_helper(kwargs['url'], kwargs['category_path'])
            return

        for element in type_arr[type_index]:
            element.click()
            self._iterate_all_product_type(type_index + 1, type_arr, url=kwargs['url'], category_path=kwargs['category_path'])

    def _get_product_info_helper(self, curr_url, category_path):
        try:
            result = {}

            WebDriverWait(self.driver, self.wait_timeout).until(
                ec.visibility_of_element_located((By.CLASS_NAME, "_44qnta")))
            product_name = self.driver.find_element(By.CLASS_NAME, '_44qnta').text
            logger.info('Product name extracted')

            price = self.driver.find_element(By.CLASS_NAME, 'pqTWkA').text
            logger.info('Price extracted')

            # this element contains average ratings, number of reviews, number sold
            avg_rating = None
            num_review = None
            num_sold = None
            some_info = self.driver.find_element(By.CLASS_NAME, 'flex.X5u-5c').text.split('\n')
            if len(some_info) == 6:  # has all info
                avg_rating = some_info[0]
                logger.info('Average rating extracted')
                num_review = some_info[1]
                logger.info('Number of ratings extracted')
                num_sold = some_info[3]
                logger.info('Number of product sold extracted')
            elif len(some_info) == 4:  # has no review
                logger.info('Product has no review')
                num_sold = some_info[3]
                logger.info('Number of product sold extracted')
            else:
                logger.info('Cannot parse review and number sold info')

            shipping = None
            try:
                shipping = self.driver.find_element(By.CLASS_NAME, 'WZTmVh').text.split('\n')[-1]
                logger.info('Shipping info extracted')
            except NoSuchElementException:
                logger.info('No shipping info found')

            attrs = {}
            type_ele = self.driver.find_element(By.CLASS_NAME, 'flex.rY0UiC.j9be9C')
            for ele in type_ele.find_elements(By.XPATH, './div/div[@class=\'flex items-center\']'):
                attr_name = ele.find_element(By.CLASS_NAME, 'oN9nMU').text
                try:
                    attr_value = ele.find_element(By.CLASS_NAME, 'product-variation.product-variation--selected')
                    attr_value = attr_value.text
                except NoSuchElementException:
                    logger.info(f"Attribute of \"{attr_name}\" is not found, setting as null")
                    attr_value = None
                attrs[attr_name] = attr_value

            product_desc = self.driver.find_element(By.CLASS_NAME, 'product-detail.page-product__detail').get_attribute(
                'innerHTML')
            logger.info('Product description extracted')

            shop_info = None
            try:
                shop_info = self.driver.find_element(By.CLASS_NAME, 'NLeTwo.page-product__shop').text
                logger.info('Shop info extracted')
            except NoSuchElementException:
                logger.info('Shop info not found')

            result['product_name'] = product_name
            result['shipping'] = shipping
            result['shop_info'] = shop_info
            result['price'] = price
            result['product_desc'] = product_desc
            result['avg_rating'] = avg_rating
            result['num_review'] = num_review
            result['num_sold'] = num_sold
            result['attrs'] = attrs
            result['url'] = curr_url

            with open(os.path.join(category_path, 'product.ndjson'), 'a') as f:
                json.dump(result, f, ensure_ascii=False)
                f.write('\n')
        except (NoSuchElementException, TimeoutException) as e:
            logger.error(e)
