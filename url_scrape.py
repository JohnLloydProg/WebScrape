from gig_scrape import GigScraping
from giglist_scrape import ListScraping
from dotenv import load_dotenv
import threading
import time
import os

load_dotenv()

CATEGORIES = {
    'programming-tech': ['website-development', 'software-development', 'mobile-app-services', 'blockchain-cryptocurrency'],
    'graphics-design': [
        'creative-logo-design', 'website-design', 'digital-illustration', 'architectural-design-services', 'product-design-services', 'image-editing', 
        'flyer-design', 'product-packaging-design', 'social-media-design', 't-shirts', ''
    ],
    'online-marketing': ['seo-services', 'social-marketing', 'online-video-marketing', 'marketing-strategy', 'music-promotion'],
    'writing-translation': ['articles-blogposts', 'proofreading-editing', 'book-and-ebook-writing', 'quality-translation-services', 'tone-of-voice', 'resume-writing'],
    'video-animation': [
        'video-editing', 'short-video-ads', 'animated-explainer-videos', 'animated-characters-modeling', '3d-product-animation', 'logo-animation-services',
        'filmed-video-production'
    ],
    'music-audio': ['producers', 'mixing-mastering', 'voice-overs', 'online-music-lessons', 'sound-design'],
    'business': ['business-registration', 'virtual-assistant-services', 'intellectual-property-management', 'sales']
}

def list_scraping(urls:list, category, subcategory, running_threads:list):
    scraping = ListScraping(os.environ.get('API_KEY'), category, subcategory, max_pages=10)
    urls.extend(scraping.get_gig_urls())
    running_threads.remove(subcategory)
    return 0


running_threads = []
urls = []
for category, subcategories in CATEGORIES.items():
    print(f'Starting to scrape gigs under the {category} category.')
    for subcategory in subcategories:
        while (len(running_threads) == 10):
            print('waiting for available thread')
            time.sleep(1)
        print(f'starting gig url scraping for {subcategory}')
        scraping_thread = threading.Thread(target=list_scraping, args=(urls, category, subcategory, running_threads), daemon=True)
        scraping_thread.start()
        running_threads.append(subcategory)

while (len(running_threads)):
    time.sleep(1)

urls = list(set(urls))

with open('urls.txt', 'w') as f:
    for url in urls:
        f.write(f'{url}\n')
