import scrapy
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup

class BollywikiSpiderSpider(scrapy.Spider):
    name = "bollywiki_spider"
    allowed_domains = ["en.wikipedia.org"]

    urls = pd.read_csv('data/urls.csv')
    start_urls = urls['start_urls'].tolist()

    def parse(self, response):
        self.logger.warning(f"response: {response.url}")
        
        # tables = response.xpath('//table[@class="wikitable"]')
        tables = response.xpath('//table[contains(@class, "wikitable")]')
        self.logger.warning(f"# tables: {len(tables)}")

        item_list = []
        for table in tables:
            items = self.process_table(table, response)
            for item in items:
                item_list.append(item)
                yield item


    def process_table(self, table, response):
        table = self.preprocess_html(table) # fix ul issue

        df = pd.read_html(StringIO(table))[0] # string to dataframe
        self.logger.warning(f"df shape: {df.shape}")  # rows, columns
        self.logger.warning(f"df columns: {df.columns}") # column names
        
        self.logger.info(f"table: {df}") # at a lower level, there if you want to see it default hidden


        # some checks to see if the table is valid
        if self.df_skip(df):
            return

        # basic transformations
        year = response.url.split('_')[-1] 
        df['year'] = year

        # Filter rows with more than 5 null values
        # sometimes the table picks up ghost rows at the bottom
        df = df[df.isnull().sum(axis=1) <= 3]

        df = self.standardize_column_names(df)
        self.logger.warning(f"df columns after standardizing: {df.columns}") # column names
        self.logger.warning(f"df after standard:\n{df.head()}") # column names

        # Filter rows where the string length is more than 6
        # bug in the wiki data. one table has a row with a long string
        # Should be "J U N" but is "Good Luck Jerry"
        # or keep if the opening_month is empty (2007 and prior data)
        df[df['opening_month'].fillna('').apply(lambda x: len(x) <= 6 or x == '')]
        
        self.logger.warning(f"df shape before transformations: {df.shape}") # column names
        df = self.df_transformations(df)
        self.logger.warning(f"df shape after transformations:\n{df.head()}") # column names

        for _, row in df.iterrows():
            item = BollywikiScraperItem()
            item['title'] = row['title']
            item['director'] = row['director']
            item['cast'] = row['cast']

            item['opening_year'] = row['year']
            item['opening_date'] = row['date']

            item['genre'] = row['genre']

            self.logger.warning(f"item: {item}")
            yield item

    def df_skip(self, df):
        df = df.copy()
        # if df doesnt have headers, only numbers, use first row as header
        if df.columns.tolist() == list(range(len(df.columns))):
            df.columns = df.iloc[0]
            df = df.iloc[1:]

        # skip if table has too few rows
        if df.shape[0] < 2:
            self.logger.warning(f"df shape: {df.shape}. too few rows. skipping.")
            return True

        # skip if table has columns with the work "Rank" or "Gross"
        if any('rank' in col.lower() for col in df.columns): 
            self.logger.warning(f"df columns: {df.columns}. has Rank or Gross. skipping.")
            return True
        
        if any('number' in col.lower() for col in df.columns): 
            self.logger.warning(f"df columns: {df.columns}. has Rank or Gross. skipping.")
            return True
        
        if any('no.' in col.lower() for col in df.columns): 
            self.logger.warning(f"df columns: {df.columns}. has Rank or Gross. skipping.")
            return True
        
        if any('gross' in col.lower() for col in df.columns):
            self.logger.warning(f"df columns: {df.columns}. has Rank or Gross. skipping.")
            return True
        
        return False


    def standardize_column_names(self, df):
        common_column_names = {

            'Title':'title',
            'Director':'director',
            'Cast':'cast',
        }
        df = df.rename(columns=common_column_names)

        columns = df.columns.tolist()

        # 'Opening':'opening_month',
        if 'Opening' in columns:
            column_names = {'Opening':'opening_month'}
            df = df.rename(columns=column_names)
        else:
            df['opening_month'] = ''

        # 'Opening.1':'opening_day',
        if 'Opening.1' in columns:
            column_names = {'Opening.1':'opening_day'}
            df = df.rename(columns=column_names)
        else:
            df['opening_day'] = ''

        # studio
        if 'Production house' in columns:
            column_names = {'Production house':'studio',}
            df = df.rename(columns=column_names)
        elif 'Studio (production house)' in columns:
            column_names = {'Studio (production house)': 'studio'}
            df = df.rename(columns=column_names)
        else:
            df['studio'] = ''

        # ref
        if 'Ref.' in columns:
            column_names = {'Ref.':'ref'}
            df = df.rename(columns=column_names)
        elif 'Source' in columns:
            column_names = {'Source':'ref'}
            df = df.rename(columns=column_names)
        else:
            df['ref'] = ''
        
        # genre
        if 'Genre' in columns:
            column_names = {
                'Genre':'genre',
            }
            df = df.rename(columns=column_names)
        else:
            df['genre'] = ''

        return df


    def df_transformations(self, df):

        # hanlde missing values
        subset = ['title', 'director', 'cast', 'studio', 'ref']
        df.loc[:, subset] = df.loc[:, subset].fillna('')

        subset = ['opening_month', 'opening_day']
        df.loc[:, subset] = df.loc[:, subset].fillna('1')

        # date processing
        df['opening_day'] = pd.to_numeric(df['opening_day'], errors='coerce').fillna(1)
        df['opening_day'] = df['opening_day'].apply(int).apply(str)

        # data issues cause the month to be weird. 
        # sometimes it's a number, sometimes it's a string the column values slide around
        # replace all numbers with empty string and then NOne
        df['opening_month'] = df['opening_month'].str.replace(' ', '')
        df['opening_month'] = df['opening_month'].str.replace('[0-9]+', 'bad', regex=True)
        df = df.applymap(lambda x: None if 'bad' in str(x) else x)
        # filter out rows where opening_month is null
        df = df[df['opening_month'].notnull()]

        dates = df.loc[:, ['opening_month', 'opening_day', 'year']].copy() # ['JAN', '1', '2024']
        dates = dates.apply(lambda x: x.tolist(), axis=1) # ['JAN','1','2024']
        dates = dates.apply(lambda x: ','.join(x)) # 'JAN,1,2024'

        dates = pd.to_datetime(dates, format='%b,%d,%Y', errors='coerce') # pandas date object
        df['date'] = dates.dt.strftime('%Y-%m-%d') # date obj to string

        df['genre'] = df['genre'].fillna('')
        df['genre'] = df['genre'].str.replace('/', ',')
        df['genre'] = df['genre'].str.lower()

        return df

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
    
    genre = scrapy.Field()