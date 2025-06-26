from fiverr_api import session
from bs4 import BeautifulSoup
import emoji

class GigScraping:
    def __init__(self, url, api_key):
        self.url = url
        session.set_scraper_api_key(api_key)
        response = session.get(url)
        self.json = response.props_json()
        self.soup:BeautifulSoup = response.soup
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
        return description
    
    def gig_ratings(self):
        ratings = self.gig_page.find('div', {'class': 'breakdown-wrapper'})
        return ratings
    
    def gig_reviews(self):
        reviews = []
        for review in self.gig_page.find('ul', {'class':'review-list'}).find_all('div', {'class': 'review-description'}):
            if (review.string):
                words = list(filter(lambda word: word not in emoji.EMOJI_DATA, review.string.split(' ')))
                review.string = ' '.join(words)
                reviews.append(review)
        return reviews
    
    def seller_loyalty_banner(self):
        return self.gig_page.find('div', {'class': 'seller-loyalty-banner'})

    def meta_data(self):
        meta_data = self.gig_page.find('ul', {'class': 'metadata'})
        return meta_data

    def packages_table(self):
        packages = self.gig_page.find('div', {'class': 'gig-page-packages-table'})
        return packages

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
    scraping = GigScraping('https://www.fiverr.com/samridhsrivasta/create-python-bots-scripts-automate-jobs?context_referrer=search_gigs_with_modalities&source=choice_modalities_pricing&ref_ctx_id=d222c15bec434f3481e385d10148e236&fiverr_choice=true&pckg_id=1&pos=1&context_type=auto&funnel=d222c15bec434f3481e385d10148e236&seller_online=true&imp_id=d1c85349-0e65-48e5-a987-beef7dbf9918', '61bfedda36cb272d54ba91addcc3baf2')
    scraping.write('./testing.txt')