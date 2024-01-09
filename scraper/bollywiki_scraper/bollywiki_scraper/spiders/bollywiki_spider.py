import scrapy
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup



class BollywikiSpiderSpider(scrapy.Spider):
    name = "bollywiki_spider"
    allowed_domains = ["en.wikipedia.org"]

    # urls = pd.read_csv('data/urls.csv')
    # start_urls = urls['start_urls'].tolist()
    start_urls = ['https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024']
    
    def parse(self, response):
        self.logger.warning(f"response: {response.url}")
        
        tables = response.xpath('//table[@class="wikitable"]')
        self.logger.warning(f"# tables: {len(tables)}")

        # save items in a list
        items_list = []
        for table in tables:
            items = self.process_table(table, response)
            for item in items:
                # items_list.append(item)
                yield item


    def process_table(self, table, response):
        table = self.preprocess_html(table) # fix ul issue
        df = pd.read_html(StringIO(table))[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        # transformations
        # round 3
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        # round 8
        # df = df.fillna('')
        subset = ['title', 'director', 'cast', 'studio', 'ref']
        df.loc[:, subset] = df.loc[:, subset].fillna('')

        subset = ['opening_month', 'opening_day']
        df.loc[:, subset] = df.loc[:, subset].fillna('1')
        
        self.logger.warning(f"table: {df}")

        # round 7
        df['year'] = response.url.split('_')[-1] 
        df['opening_day'] = df['opening_day'].apply(int).apply(str)
        df['opening_month'] = df['opening_month'].str.replace(' ', '')

        df['date'] = df.loc[:, ['opening_month', 'opening_day', 'year']].apply(lambda x: ','.join(x.tolist()), axis=1)
        df['date'] = pd.to_datetime(df['date'], format='%b,%d,%Y')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        for i, row in df.iterrows():
            item = BollywikiScraperItem()
            # round 4
            item['title'] = row['title']
            # round 5
            item['director'] = row['director']
            # round 6
            item['cast'] = row['cast']
            # round 7
            item['opening_year'] = row['year']
            item['opening_date'] = row['date']

            self.logger.warning(f"item: {item}")
            yield item

    def preprocess_html(self, table):
        table = table.get() # selector to string

        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(table, 'html.parser')

        # Find all unordered lists
        unordered_lists = soup.find_all('ul')

        for ul in unordered_lists:
            # Extract text from each list item and join them with a comma
            text = ','.join(li.get_text() for li in ul.find_all('li'))
            # Replace the ul contents with the comma-separated text
            ul.string = text

        modified_html = str(soup)
        return modified_html



class BollywikiScraperItem(scrapy.Item):
    title = scrapy.Field()
    director = scrapy.Field()
    cast = scrapy.Field()

    opening_year = scrapy.Field()
    opening_date = scrapy.Field()

