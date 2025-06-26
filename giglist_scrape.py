from fiverr_api import session
from bs4 import BeautifulSoup
from gig_scrape import GigScraping


class ListScraping:
    def __init__(self, api_key, category, subcategory, max_pages=20):
        self.category = category
        self.subcategory = subcategory
        self.max_pages = max_pages
        session.set_scraper_api_key(api_key)
    
    def get_gig_urls(self) -> list:
        urls = set()
        try:
            for page_no in range(1, 1+self.max_pages):
                response = session.get(f'https://www.fiverr.com/categories/{self.category}/{self.subcategory}?source=pagination&filter=rating&page={str(page_no)}&limit=48')
                soup:BeautifulSoup = response.soup
                json:dict = response.props_json()
                for gig in response.props_json().get('listings')[0].get('gigs'):
                    urls.add(gig.get('gig_url'))
                print(f'Done with page {str(page_no)} out of 20')
        except:
            print(f'Error in scraping the urls for the {self.subcategory} under the {self.category} category')
        return list(urls)

if (__name__ == '__main__'):
    urls = ListScraping('61bfedda36cb272d54ba91addcc3baf2', 'programming-tech', 'website-development').get_gig_urls()

    for url in urls:
        print(url)

    print(f'Number of gig links: {str(len(urls))}')