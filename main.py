import logging
import argparse
from scraper.lazada_scraper import LazadaScraper
from scraper.shopee_scraper import ShopeeScraper


import os
os.chdir(os.path.dirname(__file__))

logger = logging.getLogger('scraper')
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

fh = logging.FileHandler('log.txt')
fh.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)

if __name__ == '__main__':
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='E-commerce data scraper')

    parser.add_argument('--site', type=str,
                        help='The website to scrape from. Values can be lazada or shopee')
    parser.add_argument('--type', type=str,
                        help='What kind of data to scrape. Values can be url or info')
    parser.add_argument('--num_page', type=int,
                        help='How many pages to scrape urls from. Only effective when type is url')
    parser.add_argument('--headless', action='store_true',
                        help='Run browser in the background')
    parser.add_argument('--login', action='store_true',
                        help='Run browser in the background')

    args = parser.parse_args()

    if args.site == 'shopee':
        if args.num_page:
            scraper = ShopeeScraper(num_page_to_scrape=args.num_page, is_headless=args.headless)
        else:
            scraper = ShopeeScraper(is_headless=args.headless)

        if args.login:
            scraper.get_main_page()

        if args.type == "url":
            scraper.get_product_urls()
        elif args.type == "info":
            scraper.get_product_info()

    elif args.site == 'lazada':
        if args.num_page:
            scraper = LazadaScraper(num_page_to_scrape=args.num_page, is_headless=args.headless)
        else:
            scraper = LazadaScraper(is_headless=args.headless)

        if args.login:
            scraper.get_main_page()

        if args.type == "url":
            scraper.get_product_urls()
        elif args.type == "info":
            scraper.get_product_info()
