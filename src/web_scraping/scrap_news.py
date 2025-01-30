import datetime, json, os, html2text, re, spacy
from langchain.document_loaders import AsyncHtmlLoader
from langchain.document_transformers import Html2TextTransformer
import pandas as pd


async def create_db(news_links=None):
    final_news = []
    news_links = get_links() if news_links==None else news_links

    for source, news in news_links.items():
        print(source)
        for company, search_results in news.items():
            print(f'    {company}')
            scraper = scrap_news(company, source)
            await scraper.structure_db(search_results)
            final_news.extend(scraper.news_docs)
    save_db(final_news)

def get_links():
    week_number = datetime.datetime.now().isocalendar()[1]
    path = os.getcwd()
    path = os.path.join(path, 'engine_search', f'week_{week_number}.json')
    with open(path, 'r') as f:
        news_links = json.loads(f.read())
    return news_links

async def do_webscraping(urls):
    try:
        loader = AsyncHtmlLoader(urls, ignore_load_errors=True)
        docs = loader.load()

        html2text_transformer = Html2TextTransformer()
        docs_transformed = html2text_transformer.transform_documents(docs)

        news = []
        for new in docs_transformed:
            if new != None:
                metadata = new.metadata
                title = metadata.get('title', '')
                news.append({
                    'summary': new.page_content,
                    'title': title,
                    'metadata': metadata,
                    'clean_content': html2text.html2text(new.page_content)
                })
        return news

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

async def get_news(search_results):
  structured_response = await do_webscraping(search_results)
  return structured_response

async def process_news(structured_response):
  news = {}
  for i, doc in enumerate(structured_response):
    doc = doc['clean_content'].replace(doc['title'], '')
    texts = pd.Series(re.split('#', doc)).str.strip()
    texts = texts[texts.str.len()>100].reset_index(drop=True)
    lens = texts.apply(lambda text: pd.Series(text.split('\n')).str.strip().str.len().mean())
    texts = texts[lens>20]
    news[structured_response[i]['title'].strip()] = texts
  return news

def get_dates(news):
    dates = []
    for value in news.values():
        date = value[value.str.contains('[0-9]{1,4}\W[0-9]{1,2}\W[0-9]{1,4}', regex=True)]
        date = date.str.extract('([0-9]{1,4}\W[0-9]{1,2}\W[0-9]{1,4})')
        date = date[0].str.split('\W', regex=True, expand=True)
        if len(date):
            lens = date.iloc[0].str.len()
            if lens.tolist()==lens.sort_values(ascending=False).tolist():
                date_ = '-'.join(date.iloc[0].tolist())
            else:
                date_ = '-'.join(date.iloc[0][::-1].tolist())
        else:
            now = datetime.datetime.now()
            date_ = f'{now.year}-{0 if now.month<10 else ""}{now.month}-{0 if now.day<10 else ""}{now.day}'
        dates.append(date_)
    return dates

class scrap_news:
    nlp = spacy.load('es_core_news_md')

    def __init__(self, company, source):
        self.company, self.source = company, source

    def cos_sim(self, text1, text2):
        tokens1 = self.nlp(text1)
        tokens2 = self.nlp(text2)
        similarity = tokens1.similarity(tokens2)
        return similarity

    def get_similarity(self, df, title, cutoff=0.4):
        embedds = []
        for text in df.tolist():
            embedds.append(self.cos_sim(text, title))
        df = df.to_frame().assign(SIMILARITY=embedds)
        df = df[df.SIMILARITY>cutoff]
        return df

    def scrap(self, news):
        self.similarity, self.content = {}, []
        for key, value in news.items():
            self.similarity[key] = self.get_similarity(value, key)
            value = self.similarity[key][0]
            value = value.str.replace('(\n)|(\*)', ' ', regex=True).str.replace(' +', ' ', regex=True).str.strip()
            self.content.append('\n\n\n\n'.join(value.tolist()))

    def process_results(self, dates):
        news_docs = []
        for i in range(len(self.content)):
            doc = {
                'content': self.content[i],
                'metadata': {
                    'title': self.structured_response[i]['title'],
                    'source': self.source,
                    'link': self.structured_response[i]['metadata']['source'],
                    'date': dates[i],
                    'company': self.company
                }
            }
            news_docs.append(doc)
        self.news_docs = news_docs
    
    async def structure_db(self, search_results):
        self.structured_response = await get_news(search_results)
        news = await process_news(self.structured_response)
        self.scrap(news)
        dates = get_dates(news)
        self.process_results(dates)

def save_db(final_news):
    db = {'company': [], 'source': [], 'date': [], 'title': [], 'content': [], 'link': []}
    for new in final_news:
        for key in db.keys():
            value = new[key] if key=='content' else new['metadata'][key]
            db[key].append(value)
    db = pd.DataFrame(db)
    week_number = datetime.datetime.now().isocalendar()[1]
    db.to_csv(os.path.join('news_results', f'week_{week_number}.csv'), index=0, sep='|')