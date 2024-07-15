import asyncio, warnings
from src.commons.objects import companies, sources, sources_links
from src.web_scraping.scrap_news import create_db
from src.web_scraping.search import get_news_links

warnings.simplefilter('ignore')


def do_scraping():
    print('farming links...')
    search_results = get_news_links(companies, sources, sources_links)
    print('scraping content...')
    asyncio.get_event_loop().run_until_complete(create_db(search_results))


if __name__=='__main__':
    do_scraping()