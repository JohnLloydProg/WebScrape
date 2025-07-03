import oracledb
from oracledb.connection import Connection
from dotenv import load_dotenv
from gig_scrape import GigScraping
from time import sleep
import threading
import os

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()

def list_scraping(url_id, gig_link:str, running_threads:list):
    scraping = GigScraping(gig_link, url_id)
    scraping.oracle_upload()
    running_threads.remove(url_id)
    return 0

if (__name__ == '__main__'):
    load_dotenv()

    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_8")

    os.environ['TNS_ADMIN'] = './wallet'
    running_threads = []
    with oracledb.connect(user='admin', password=os.environ['PASSWORD'], dsn=os.environ['DSN']) as connection:
        print('established connection to oracle')
        with connection.cursor() as cursor:
            urls = list(cursor.execute('select url_id, gig_link from GIG_URLS where scraped = 0 fetch next 100 rows only'))
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