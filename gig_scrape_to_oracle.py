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
    global progress, l, message
    try:
        scraping = GigScraping(gig_link, url_id)
        scraping.update_oracle()
        running_threads.remove(url_id)
        message = f'done scraping {str(scraping.url_id)}'
        with open('logs.txt', 'a') as f:
            f.write(f'{message}\n')
        progress += 1
        
        return 0
    except Exception as e:
        print(f'Error scraping {gig_link} with URL ID {url_id}: {e}')
        running_threads.remove(url_id)
        progress += 1
        return -1

def display_message_progressBar():
    global progress, l, message, running_threads
    sleep(5)
    while len(running_threads) != 0:
        printProgressBar(progress, l, prefix='Progress', suffix=f'Complete: {message}', length=50)
        sleep(0.5)

if (__name__ == '__main__'):
    load_dotenv()

    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_8")

    os.environ['TNS_ADMIN'] = './wallet'
    running_threads = []
    with oracledb.connect(user='admin', password=os.environ['PASSWORD'], dsn=os.environ['DSN']) as connection:
        print('established connection to oracle')
        with connection.cursor() as cursor:
            urls = list(cursor.execute('select u.url_id, u.gig_link from GIG_URLS u inner join GIG_DETAILS d on d.url_id = u.url_id where communication is null offset 3000 rows fetch next 3000 rows only'))
        l = len(urls)
        print(f'Found {l} URLs to scrape')
        progress = 0
        printProgressBar(0, l, prefix='Progress', suffix='Complete', length=50)
        thread = threading.Thread(target=display_message_progressBar, daemon=True)
        thread.start()
        for url in urls:
            while len(running_threads) == 10:
                sleep(1)
            url_id, url = url
            url = f'https://www.fiverr.com{url}'
            message = f'Running thread for {url} with id {url_id}'
            with open('logs.txt', 'a') as f:
                f.write(f'{message}\n')
            thread = threading.Thread(target=list_scraping, args=(url_id, url, running_threads), daemon=True)
            thread.start()
            running_threads.append(url_id)
    while len(running_threads):
        sleep(1)
        print(f'Waiting for {len(running_threads)} threads to finish...')