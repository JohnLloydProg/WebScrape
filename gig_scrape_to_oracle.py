import oracledb
from oracledb.connection import Connection
from dotenv import load_dotenv
from gig_scrape import GigScraping
from time import sleep
import threading
import os


def list_scraping(url_id, gig_link:str, running_threads:list):
    try:
        scraping = GigScraping(gig_link, url_id)
        scraping.oracle_upload()
        running_threads.remove(url_id)
        return 0
    except Exception as e:
        print(f'Error scraping {gig_link} with URL ID {url_id}: {e}')
        running_threads.remove(url_id)
        return -1

if (__name__ == '__main__'):
    load_dotenv()

    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_8")

    os.environ['TNS_ADMIN'] = './wallet'
    running_threads = []
    with oracledb.connect(user='admin', password=os.environ['PASSWORD'], dsn=os.environ['DSN']) as connection:
        print('established connection to oracle')
        with connection.cursor() as cursor:
            urls = list(cursor.execute('select url_id, gig_link from GIG_URLS where scraped = 0 offset 4500 rows fetch next 1000 rows only'))
        l = len(urls)
        print(f'Found {l} URLs to scrape')
        for url in urls:
            while len(running_threads) == 10:
                sleep(1)
                print('wating for available thread')
            url_id, url = url
            url = f'https://www.fiverr.com{url}'
            print('Running thread for', url, 'with id', url_id)
            thread = threading.Thread(target=list_scraping, args=(url_id, url, running_threads), daemon=True)
            thread.start()
            running_threads.append(url_id)
    while len(running_threads):
        sleep(1)
        print(f'Waiting for {len(running_threads)} threads to finish...')