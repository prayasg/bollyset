Parts:
- [[Bollyset Scraper Part 1 - Getting Started]]
- [[Bollyset Scraper Part 2 - Transformations]]
- [[Bollyset Scraper Part 3 - 2023 and csv]]
- [[Bollyset Scraper Part 4 - More Years]]
- [[Bollyset Scraper Part 5 - 2017 table version]]
- [[Bollyset Scraper Part 6 - 2007 and prior]]

## 2022
We know 2024, 2023 work. Lets remove them for now. 

Look through the wiki pages for the years. The table format stays consistent till 2017. 

So:
- 2017-2024 our script works. 
- ? - 2016 will need a script mod

## Spider code for 2017-2024

```
import scrapy
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup

class BollywikiSpiderSpider(scrapy.Spider):
    name = "bollywiki_spider"
    allowed_domains = ["en.wikipedia.org"]

    # urls = pd.read_csv('data/urls.csv')
    # start_urls = urls['start_urls'].tolist()
    start_urls = [
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2023',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2022',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2021',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2020',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2019',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2018',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2017',
    ]


    def parse(self, response):
        self.logger.warning(f"response: {response.url}")
        
        tables = response.xpath('//table[@class="wikitable"]')
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

        if df.shape[1] < 4:
            return

        # transformations
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        # hanlde missing values
        subset = ['title', 'director', 'cast', 'studio', 'ref']
        df.loc[:, subset] = df.loc[:, subset].fillna('')

        subset = ['opening_month', 'opening_day']
        df.loc[:, subset] = df.loc[:, subset].fillna('1')

        # date processing
        df['year'] = response.url.split('_')[-1] 
        df['opening_day'] = df['opening_day'].apply(int).apply(str)
        df['opening_month'] = df['opening_month'].str.replace(' ', '')

        dates = df.loc[:, ['opening_month', 'opening_day', 'year']].copy() # ['JAN', '1', '2024']
        dates = dates.apply(lambda x: x.tolist(), axis=1) # ['JAN','1','2024']
        dates = dates.apply(lambda x: ','.join(x)) # 'JAN,1,2024'

        dates = pd.to_datetime(dates, format='%b,%d,%Y') # pandas date object
        df['date'] = dates.dt.strftime('%Y-%m-%d') # date obj to string

        for i, row in df.iterrows():
            item = BollywikiScraperItem()
            item['title'] = row['title']
            item['director'] = row['director']
            item['cast'] = row['cast']

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

```

stores 606 items. 

So are we good? Hard to say. 

## Are we good?
Hard to say. 

### Checking random rows.
Ill start by spot checking. Pick random entries out of the csv and look them up on the wiki pages. 

You should do something like this every time you create a csv to do something with.

How many records should you verify? Start with 2%. Keep washing till the water comes back clean. 

2% of 600 = 12. Move forward if 12/12 come back clean. 

Make sure to do the other way too. Wiki -> csv.

### Did we get all rows?
Pick a random year. Count the movies on the wiki page and compare them to entries in the csv. 

#### Problem 1: missing 2017
The whole year is missing

Lets check 2018.
CSV has 48 entries. 
Counting the wiki by hand there should be 100+. Whoops

#### Problem 2: 2018 incomplete
We could probably keep finding issues but lets fix these two for now. 

## Fixing Problem 1

This one is easy. The table is actually different. There is a new column, genre. Lets handle this in the next part. 

## Fixing Problem 2
This one also seems to be fine. The spider stats say 104 items were scraped. I just dont know how to use excel. Whoops

## Conclusion
Our spider currently works for the following `start_urls`

```
    start_urls = [
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2023',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2022',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2021',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2020',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2019',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2018',
    ]

```

saved this as `bollyset_2018_2024.csv`

## Next steps
Lets hit the next set of years. This table format, just eyeballing, adds a new column `genre`. How do we handle this? Lets find out!

[[Bollyset Scraper Part 5 - 2017 table version]]
