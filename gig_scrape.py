from oracledb.connection import Connection
from fiverr_api import session
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import oracledb
import emoji
import os

class GigScraping:
    def __init__(self, url, url_id=None):
        session.set_scraper_api_key(os.environ.get('API_KEY'))
        response = session.get(url)
        self.url_id = url_id
        self.soup:BeautifulSoup = response.soup
        self.gig_page = self.get_gig_page()
        print('initialized GigScraping with URL ID:', self.url_id)
    
    def get_gig_page(self):
        return self.soup.find('div', {"class":'gig-page'})
    
    def gig_overview(self):
        overview = self.gig_page.find('div', {'class': 'gig-overview'})
        if (overview):
            title = overview.find('h1')
            if (title):
                self.title = title.string
            else:
                self.title = 'Unknown Title'
            seller_overview = overview.find('div', {'class': 'seller-overview'})
            if (seller_overview):
                seller = seller_overview.find('div', {'class': 'flex'})
                if (seller):
                    self.seller = seller.string
                else:
                    self.seller = 'Unknown Seller'
        print('finisehd gig overview with title:', self.title, 'and seller:', self.seller)
        return overview
    
    def gig_description(self):
        description = self.gig_page.find('div', {'class': 'description-content'})
        self.description = ''
        if (description):
            paragraphs = description.children
            for paragraph in paragraphs:
                if (paragraph.string):
                    self.description += f'{paragraph.string}\n'
                if (paragraph.find('ul')):
                    list_elements = paragraph.find_all('li')
                    if (list_elements):
                        for list_element in list_elements:
                            self.description += f'{list_element.string}\n'
        self.description = self.description.strip()
        self.description = self.description.replace("'", "").replace('"', '')  # Escape single quotes for SQL
        self.description = self.description.replace(',', '')
        print('finished gig description with length:', len(self.description))
        return description
    
    def gig_ratings(self):
        ratings = self.gig_page.find('div', {'class': 'breakdown-wrapper'})
        rating = ratings.find('strong', {'class': 'rating-score'})
        if (rating):
            self.rating = float(rating.string)
        else:
            self.rating = 0.0
        print('finished gig ratings with rating:', self.rating)
        return ratings
    
    def gig_reviews(self):
        reviews = []
        self.repeat_customers = 0
        spans = self.gig_page.find('ul', {'class':'review-list'}).find_all('span', recursive=False)
        if not spans:
            print('No reviews found for this gig')
            return []
        for review in spans:
            review_content = review.find('div', {'class': 'review-description'})
            print(review_content)
            rating_str = review.find('strong', {'class': 'rating-score'})
            print(rating_str)
            rating = float(rating_str.string) if (rating_str) else 0
            if 'Repeat Client' in [par.string for par in review.find_all('p')]:
                self.repeat_customers += 1
            if (review_content):
                if (review_content.string):
                    words = list(filter(lambda word: word not in emoji.EMOJI_DATA, review_content.string.split(' ')))
                    review_content.string = ' '.join(words)
                    print(review_content.string, rating)
                    reviews.append((review_content.string, rating))
        self.number_of_reviews = len(reviews)
        return reviews
    
    def seller_loyalty_banner(self):
        return self.gig_page.find('div', {'class': 'seller-loyalty-banner'})

    def oracle_upload(self):
        os.environ['TNS_ADMIN'] = './wallet'
        with oracledb.connect(user='admin', password=os.environ['PASSWORD'], dsn=os.environ['DSN']) as connection:
            with connection.cursor() as cursor:
                self.gig_overview()
                description = self.gig_description()
                if (not description):
                    print('No description found for the gig')
                    cursor.execute(f'update GIG_URLS set scraped = 1 where url_id = {str(self.url_id)}')
                    connection.commit()
                    return
                self.gig_ratings()
                if (not self.url_id):
                    cursor.execute(f"insert into GIG_DETAILS (gig_title, number_of_words, rating, gig_description) values ('{self.title}', {str(len(self.description))}, {str(self.rating)}, '{self.description}')")
                else:
                    cursor.execute(f"insert into GIG_DETAILS (gig_title, number_of_words, rating, gig_description, url_id) values ('{self.title}', {str(len(self.description))}, {str(self.rating)}, '{self.description}', {str(self.url_id)})")
                connection.commit()
                print('Uploaded gig details')
                cursor.execute(f'update GIG_URLS set scraped = 1 where url_id = {str(self.url_id)}')
                print('Updated GIG_URLS table')
                connection.commit()
                print('scraping reviews')
                reviews = self.gig_reviews()
                for review, rating in reviews:
                    review = review.replace("'", "").replace('"', '')  # Escape single quotes for SQL
                    self.description = self.description.replace(',', '')
                    print(review, rating)
                    cursor.execute(f"insert into GIG_REVIEWS (rating, content, url_id) values ({str(rating)}, '{review}', {str(self.url_id)})")
                connection.commit()
                cursor.execute(f'update GIG_DETAILS set reviews = {self.number_of_reviews}, repeating_customers = {self.repeat_customers} where url_id = {str(self.url_id)}')
                connection.commit()
                print(f'Uploaded {self.number_of_reviews} reviews for {self.title} with {self.repeat_customers} repeat customers')


    def write(self, file_name):
        try:
            with open(file_name, 'w') as f:
                f.write('OVERVIEW'.center(20, '=') + '\n')
                f.write(self.gig_overview().prettify())
                print('done with overview')
                f.write('DESCRIPTION'.center(20, '=') + '\n')
                f.write(self.gig_description().prettify())
                print('done with description')
                f.write('REVIEWS'.center(20, '=') + '\n')
                f.write(self.gig_ratings().prettify())
                for review in self.gig_reviews():
                    f.write(review.prettify())
                print('done with reviews')
        except WindowsError as err:
            print(f'Problem with writing to {self.url}')
            print(err)
