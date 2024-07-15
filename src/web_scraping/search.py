import datetime, json, time, re, traceback
from tqdm import tqdm
import pandas as pd
from duckduckgo_search import DDGS
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


def api_search(company, source, sources_links, sleep):
    results = DDGS().text(
                    keywords=f'{company} site:{sources_links[source]}',
                    max_results=10,
                    safesearch='off',
                    timelimit='w'
                    )
    time.sleep(3)
    return results

def selenium_search(driver, company, source, sources_links):
    driver.get(f'https://duckduckgo.com/?q={company}+site%3A{sources_links[source]}&t=h_&df=w&ia=web')
    time.sleep(1) # give page a chance to fully load
    content = driver.page_source
    links = pd.Series(re.findall(f'(<a href=".*?" rel)', content, flags=re.IGNORECASE))
    links = links.str.replace('<a href=', '').str.replace('"', '').str.replace(' rel', '').drop_duplicates()
    links = links[links.str.contains('https\://www.+', regex=True, case=False)]
    return links

def get_news_links(companies, sources, sources_links):
    search_results = {}
    pbar = tqdm(total=len(companies)*len(sources))
    driver = webdriver.Chrome()
    try:
        for source in sources:
            aux = {}
            for company in companies:
                pbar.update(1)
                pbar.set_description(f'source:{source}-company:{company}')
                results = selenium_search(driver, company, source, sources_links)
                if len(results):
                    if isinstance(results, dict):
                        aux[company] = [json_['href'] for json_ in results]
                    else:
                        aux[company] = results.tolist()
            if len(aux):
                search_results[source] = aux
        pbar.close()
        driver.quit()
        week_number = datetime.datetime.now().isocalendar()[1]
        with open(f'engine_search/week_{week_number}.json', 'w') as f:
            json.dump(search_results, f)
        return search_results
    except Exception:
        traceback.print_exc()
        print('failed scraping...')
        pbar.close()
        driver.quit()