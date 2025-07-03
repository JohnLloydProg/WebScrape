from oracledb.connection import Connection
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import emoji
import os

class GigScraping:
    def __init__(self, content, url_id=None):
        self.url_id = url_id
        self.soup:BeautifulSoup = BeautifulSoup(content, 'html5lib')
        self.gig_page = self.get_gig_page()
    
    def get_gig_page(self):
        return self.soup.find('div', {"class":'gig-page'})
    
    def gig_overview(self):
        overview = self.gig_page.find('div', {'class': 'gig-overview'})
        self.title = overview.find('h1').string
        self.seller = overview.find('div', {'class': 'seller-overview'}).find('div', {'class': 'flex'}).string
        return overview
    
    def gig_description(self):
        description = self.gig_page.find('div', {'class': 'description-content'})
        paragraphs = description.children
        self.description = ''
        for paragraph in paragraphs:
            if (paragraph.string):
                self.description += f'{paragraph.string}\n'
            list_elements = paragraph.find_all('li')
            if (list_elements):
                for list_element in list_elements:
                    self.description += f'{list_element.string}\n'
        return description
    
    def gig_ratings(self):
        ratings = self.gig_page.find('div', {'class': 'breakdown-wrapper'})
        rating = ratings.find('strong', {'class': 'rating-score'})
        if (rating.string):
            self.rating = float(rating.string)
        else:
            self.rating = 0.0
        return ratings
    
    def gig_reviews(self):
        reviews = []
        self.repeat_customers = 0
        for review in self.gig_page.find('ul', {'class':'review-list'}).find_all('span', recursive=False):
            review_content = review.find('div', {'class': 'review-description'})
            rating_str = review.find('strong', {'class': 'rating-score'}).string
            print(rating_str)
            rating = int(rating_str) if (rating_str) else 0
            if 'Repeat Client' in [par.string for par in review.find_all('p')]:
                self.repeat_customers += 1
            if (review_content.string):
                words = list(filter(lambda word: word not in emoji.EMOJI_DATA, review_content.string.split(' ')))
                review_content.string = ' '.join(words)
                reviews.append((review_content.string, rating))
        self.number_of_reviews = len(reviews)
        return reviews
    
    def seller_loyalty_banner(self):
        return self.gig_page.find('div', {'class': 'seller-loyalty-banner'})

    def oracle_upload(self, connection:Connection):
        with connection.cursor() as cursor:
            self.gig_overview()
            self.gig_description()
            self.gig_ratings()
            if (self.url_id):
                cursor.execute(f"insert into GIG_DETAILS (gig_title, number_of_words, rating) values ('{self.title}', {str(len(self.description))}, {str(self.rating)})")
            else:
                cursor.execute(f"insert into GIG_DETAILS (gig_title, number_of_words, rating, url_id) values ('{self.title}', {str(len(self.description))}, {str(self.rating)}, {str(self.url_id)})")
            connection.commit()
            cursor.execute(f'update GIG_URLS set scraped = 1 where url_id = {str(self.url_id)}')
            connection.commit()
            print('scraping reviews')
            reviews = self.gig_reviews()
            for review, rating in reviews:
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

if (__name__ == '__main__'):
    load_dotenv()
    from my_browser import Browser
    browser = Browser('http://www.fiverr.com/samridhsrivasta/create-python-bots-scripts-automate-jobs?context_referrer=search_gigs_with_modalities&source=choice_modalities_pricing&ref_ctx_id=d222c15bec434f3481e385d10148e236&fiverr_choice=true&pckg_id=1&pos=1&context_type=auto&funnel=d222c15bec434f3481e385d10148e236&seller_online=true&imp_id=d1c85349-0e65-48e5-a987-beef7dbf9918')
    scraping = GigScraping(browser.content)
    scraping.oracle_upload