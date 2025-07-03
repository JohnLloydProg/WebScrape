from oracledb.connection import Connection
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
from time import sleep
from concurrent.futures import ThreadPoolExecutor
import threading
from gig_scrape import GigScraping
import os

load_dotenv()
#'7ab94466-1b09-4ae2-b1fa-80f61cee3b09'

class Browser(threading.local):
    def __init__(self, url:str, url_id:int=None, connection:Connection=None):
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
                proxy={
                    "server": f"brd.superproxy.io:33335",
                    'username': 'brd-customer-hl_205833d9-zone-web_unlocker1',
                    'password': 'upb680dtlued'
                    },
                args=[
                    "--use-angle=vulkan",
                    "--enable-features=Vulkan",
                    "--disable-vulkan-surface",
                    "--enable-unsafe-webgpu",
                    "--enable-gpu-rasterization",
                    "--ignore-gpu-blocklist" # Added to ignore GPU blocklist, potentially enabling more features
                ]
            )
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            page.route("**/*", self.route_intercept)
            page.goto(url, timeout=0)
            reviews = page.locator('.reviews-wrap').first
            clicks = 0
            button = reviews.get_by_role('button', name='Show More Reviews')
            while button.is_visible() and clicks < 2:
                button = reviews.get_by_role('button', name='Show More Reviews')
                button.click()
                sleep(40)
                clicks += 1
            self.content = page.content()
            browser.close()
        scraping = GigScraping(self.content, url_id)
        scraping.oracle_upload(connection)
        scraping.write(f'./data/{scraping.title}.txt')

    def route_intercept(self, route):
        if route.request.resource_type == "image":
            print(f"Blocking the image request to: {route.request.url}")
            return route.abort()
        return route.continue_()
