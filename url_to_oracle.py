import oracledb
from dotenv import load_dotenv
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

if (__name__ == '__main__'):

    load_dotenv()

    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_8")

    os.environ['TNS_ADMIN'] = './wallet'

    with oracledb.connect(user='admin', password=os.environ['PASSWORD'], dsn=os.environ['DSN']) as connection:
        with connection.cursor() as cursor:
            with open('./urls.txt', 'r') as f:
                urls = f.readlines()
                l = len(urls)
                printProgressBar(0, l, prefix='Progress', suffix='Complete', length=50)
                for i, url in enumerate(urls):
                    url = url.rstrip()
                    sql = f"insert into GIG_URLS (gig_link, scraped) values ('{url}', 0)"
                    cursor.execute(sql)
                    printProgressBar(i + 1, l, prefix='Progress', suffix='Complete', length=50)
                    if (i % 20 == 0):
                        connection.commit()