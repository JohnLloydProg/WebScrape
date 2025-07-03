import pandas as pd
from dotenv import load_dotenv
import oracledb
import os

if (__name__ == '__main__'):
    load_dotenv()
    oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_8")

    os.environ['TNS_ADMIN'] = './wallet'

    with oracledb.connect(user='admin', password=os.environ['PASSWORD'], dsn=os.environ['DSN']) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM GIG_URLS u INNER JOIN GIG_DETAILS d ON u.url_id = d.url_id')
            gigs = cursor.fetchall()
            df_gigs = pd.DataFrame(gigs, columns=[col[0] for col in cursor.description])
            
            cursor.execute('SELECT * FROM GIG_URLS u inner join GIG_REVIEWS r on u.url_id = r.url_id')
            reviews = cursor.fetchall()
            df_reviews = pd.DataFrame(reviews, columns=[col[0] for col in cursor.description])
    
    print('================================================================')
    print(f'Found {len(df_gigs)} rows in GIG_DETAILS table ')
    for col in df_gigs.columns:
        print(f'[{col}] empty rows: {df_gigs[col].isnull().sum()} {df_gigs[col].dtypes}')
    
    #Removing duplicates and filling NaN values
    df_gigs['REPEATING_CUSTOMERS'] = df_gigs['REPEATING_CUSTOMERS'].fillna(0)
    df_gigs['REVIEWS'] = df_gigs['REVIEWS'].fillna(0)
    df_gigs['NUMBER_OF_WORDS'] = df_gigs['NUMBER_OF_WORDS'].fillna(0)
    df_gigs['RATING'] = df_gigs['RATING'].fillna(df_gigs['RATING'].mean())
    df_gigs = df_gigs[df_gigs['GIG_DESCRIPTION'].notnull()]
    df_gigs['GIG_DESCRIPTION'].drop_duplicates(inplace=True)

    #Normalizing quantitative columns
    df_gigs['REPEATING_CUSTOMERS_STANDARDIZED'] = (df_gigs['REPEATING_CUSTOMERS'] - df_gigs['REPEATING_CUSTOMERS'].mean()) / df_gigs['REPEATING_CUSTOMERS'].std()
    df_gigs['REVIEWS_STANDARDIZED'] = (df_gigs['REVIEWS'] - df_gigs['REVIEWS'].mean()) / df_gigs['REVIEWS'].std()
    df_gigs['RATING_STANDARDIZED'] = (df_gigs['RATING'] - df_gigs['RATING'].mean()) / df_gigs['RATING'].std()
    df_gigs['NUMBER_OF_WORDS_STANDARDIZED'] = (df_gigs['NUMBER_OF_WORDS'] - df_gigs['NUMBER_OF_WORDS'].mean()) / df_gigs['NUMBER_OF_WORDS'].std()

    #Binning the rating column and number of words
    df_gigs['RATING_BINNED'] = pd.cut(df_gigs['RATING'], bins=[0, 2, 4, 5], labels=['Worst', 'Acceptable', 'Best'])
    df_gigs['NUMBER_OF_WORDS_BINNED'] = pd.cut(df_gigs['NUMBER_OF_WORDS'], bins=[0, 100, 500, 1000, 5000], labels=['Short', 'Medium', 'Long', 'Very Long'])

    print('================================================================')
    print(df_gigs.describe())
    df_gigs.to_csv('gigs_cleaned.csv')
    df_gigs.to_excel('gigs_cleaned.xlsx')

    print('\n\n================================================================')
    print(f'Found {len(df_reviews)} rows in GIG_REVIEWS table ')
    for col in df_reviews.columns:
        print(f'[{col}] empty rows: {df_reviews[col].isnull().sum()} {df_reviews[col].dtypes}')
    
    #Removing duplicates and filling NaN values
    df_reviews['RATING'] = df_reviews['RATING'].fillna(df_reviews['RATING'].mean())
    df_reviews = df_reviews[df_reviews['CONTENT'].notnull()]
    df_reviews['CONTENT'].drop_duplicates(inplace=True)


    print('================================================================')
    print(df_reviews.describe())
    df_reviews.to_csv('reviews_cleaned.csv')
    df_reviews.to_excel('reviews_cleaned.xlsx')


    print('Data cleaning completed and saved to gigs_cleaned.csv and gigs_cleaned.xlsx')



    


