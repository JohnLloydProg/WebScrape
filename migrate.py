import oracledb
from oracledb.connection import Connection
from dotenv import load_dotenv
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

def migrate(i, gig, running_threads:list):
    global progress
    with oracledb.connect(user='admin', password=os.environ['PASSWORD'], dsn=os.environ['DSN']) as connection:
        with connection.cursor() as cursor:
            url_id, quality, value_of_delivery, reviews, rating, number_of_words, repeating_customers, communication = gig
            if (quality != None):
                cursor.execute(f'update GIG_URLS set QUALITY={str(quality)} where URL_ID={str(url_id)}')
            if (value_of_delivery != None):
                cursor.execute(f'update GIG_URLS set VALUE_OF_DELIVERY={str(value_of_delivery)} where URL_ID={str(url_id)}')
            if (reviews != None):
                cursor.execute(f'update GIG_URLS set REVIEWS={str(reviews)} where URL_ID={str(url_id)}')
            if (rating != None):
                cursor.execute(f'update GIG_URLS set RATING={str(rating)} where URL_ID={str(url_id)}')
            if (number_of_words != None):
                cursor.execute(f'update GIG_URLS set NUMBER_OF_WORDS={str(number_of_words)} where URL_ID={str(url_id)}')
            if (repeating_customers != None):
                cursor.execute(f'update GIG_URLS set REPEATING_CUSTOMERS={str(repeating_customers)} where URL_ID={str(url_id)}')
            if (communication != None):
                cursor.execute(f'update GIG_URLS set COMMUNICATION={str(communication)} where URL_ID={str(url_id)}')
        connection.commit()
    running_threads.remove(i)
    progress += 1

def display_message_progressBar(l, running_threads:list):
    global progress
    sleep(5)
    while len(running_threads) != 0:
        printProgressBar(progress, l, prefix='Progress', suffix=f'Complete', length=50)
        sleep(0.5)


if (__name__ == '__main__'):
    load_dotenv()

    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_8")

    os.environ['TNS_ADMIN'] = './wallet'
    with oracledb.connect(user='admin', password=os.environ['PASSWORD'], dsn=os.environ['DSN']) as connection:
        print('established connection to oracle')
        with connection.cursor() as cursor:
            gigs = list(cursor.execute('select d.URL_ID, d.QUALITY, d.VALUE_OF_DELIVERY, d.REVIEWS, d.RATING, d.NUMBER_OF_WORDS, d.REPEATING_CUSTOMERS, d.COMMUNICATION from GIG_DETAILS d inner join GIG_URLS u on u.URL_ID = d.URL_ID where u.QUALITY is null and d.QUALITY is not null'))
    
    printProgressBar(0, len(gigs), prefix='Progress', suffix=f'Complete', length=50)
    running_threads = []
    progress = 0
    thread = threading.Thread(target=display_message_progressBar, args=(len(gigs), running_threads), daemon=True)
    thread.start()
    for i, gig in enumerate(gigs):
        while len(running_threads) == 10:
            sleep(1)
        thread = threading.Thread(target=migrate, args=(i, gig, running_threads), daemon=True)
        thread.start()
        running_threads.append(i)

