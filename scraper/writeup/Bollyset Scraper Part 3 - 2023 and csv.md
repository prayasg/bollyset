Parts:
- [[Bollyset Scraper Part 1 - Getting Started]]
- [[Bollyset Scraper Part 2 - Transformations]]
- [[Bollyset Scraper Part 3 - 2023 and csv]]
- [[Bollyset Scraper Part 4 - More Years]]
- [[Bollyset Scraper Part 5 - 2017 table version]]
- [[Bollyset Scraper Part 6 - 2007 and prior]]
## 2023 and beyond
We have scraper working for the 2024 page. Lets keeps building so it can work on both 2023 and 2024. 
## Starting spider
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
    start_urls = ['https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024']
    
    def parse(self, response):
        self.logger.warning(f"response: {response.url}")
        
        tables = response.xpath('//table[@class="wikitable"]')
        self.logger.warning(f"# tables: {len(tables)}")

        for table in tables:
            items = self.process_table(table, response)
            for item in items:
                yield item


    def process_table(self, table, response):
        table = self.preprocess_html(table) # fix ul issue
        df = pd.read_html(StringIO(table))[0] # string to dataframe
        self.logger.warning(f"table: {df}")

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

change the `start_urls` to [2023](https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2023) and lets see...

errors!

## Length mismatch error
This one deserves its own section. 

To help us debug, lets add some more logging to `process_table`
```
    def process_table(self, table, response):
        table = self.preprocess_html(table) # fix ul issue
        df = pd.read_html(StringIO(table))[0] # string to dataframe
        self.logger.warning(f"df shape: {df.shape}")  # rows, columns
        self.logger.warning(f"df columns: {df.columns}") # column names
        self.logger.info(f"table: {df}") # at a lower level, there if you want to see it default hidden

```

for the erroring table:
```
2024-01-08 19:56:34 [bollywiki_spider] WARNING: df shape: (1, 2)
2024-01-08 19:56:34 [bollywiki_spider] WARNING: table:    0                                                  1
0  #  Implies that the film is multilingual and the ...
```

Odd, this table only has one column with no header detected. Look at the page you'll see what it is picking up as a table. 

Recall, in our scraper one of the transformations is updating the column headers. We hard-coded seven new column names. 
```
# transformations

column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']

df.columns = column_names
```

The error is:
`Length mismatch: Expected axis has 2 elements, new values have 7 elements`

This is not a data table we would like to parse. Fix options?
	(try to come up with these on your own...)

### Option 1: skip table if weird
Modify the script to skip the table if there are less than 7 columns detected. 
Something like: 
```
if df.shape[1] < 7:
	return
```

### Option 2: modify the xpath selector
The xpath has a filter:
`tables = response.xpath('//table[@class="wikitable"]')`

We added this to filter out other tables e.g. other languages.
Modify the script to have a more precise xpath filter

### Decision
Option 2. Why:
- the less `if`s, the better
- the less logic in process table, the better
- process table should be tied to this specific table structure. there will be other table structures. different `process_table` functions for the different table types? yuck but maybe

### implement
Well, looks like the tags dont exist. Option 2 isnt possible. `If` it is then. 

## 2023 seems to work

## Run 2023 and 2024
change `start_urls`
```
    start_urls = ['https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2023',
                  'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024',]
```

run with full logs
```
not
scrapy crawl bollywiki_spider -L WARNING

but
scrapy crawl bollywiki_spider -L INFO
```

171 items scraped. Nice! You deserve a cookie. A soft gingersnap preferably. And some chai.

## Save to csv
we talked about doing this earlier. lets do it now

scrapy has a concept called [feed exports](https://docs.scrapy.org/en/latest/topics/feed-exports.html) that writes the items to flat files. 
That link points to [item exporters](https://docs.scrapy.org/en/latest/topics/exporters.html#topics-exporters)

Both of these are inconclusive. 
Asking chatgpt
```
I am working on a scrapy project. How do i save the items my spider is producing to CSV?
```

and a well detailed answer is produced. 

solution is to add this snippit to `settings.py`
```
FEEDS = {
    'bollyset.csv': {
        'format': 'csv',
        'encoding': 'utf8',
        'store_empty': False,
        # 'fields': ['field1', 'field2'],  # specify the fields to include in the CSV, or remove to include all fields
        'overwrite': True  # set to False if you don't want to overwrite the file on each run
    },
}
```

and run the terminal command. it works!

## Review csv file
Open in excel. Looks like this:

| cast                                           | director         | opening_date | opening_year | title         |
|------------------------------------------------|------------------|--------------|--------------|---------------|
| Arjun Kapoor,Tabu,Konkona Sen Sharma,Radhika Madan,Kumud Mishra,Shardul Bhardwaj | Aasmaan Bhardwaj | 1/13/23      | 2023         | Kuttey        |
| Anshuman Jha,Riddhi Dogra,Milind Soman         | Victor Mukherjee | 1/13/23      | 2023         | Lakadbaggha   |
| Sidharth Malhotra,Rashmika Mandanna,Kumud Mishra,Sharib Hashmi | Shantanu Bagchi | 1/20/23      | 2023         | Mission Majnu |
| Rakul Preet Singh,Sumeet Vyas,Satish Kaushik,Rajesh Tailang,Dolly Ahluwalia | Tejas Deoskar | 1/20/23      | 2023         | Chhatriwali   |

Looks pretty good to me. 

**Look at every cell in every row. This will take a little bit. Its worth it.**
How many issues did you find?
- some rows are blank - dont care. fix in post
- Some titles have weird text
	- is `Adipurush[a]` supposed to be `Adipurush`?
	- unusual characters

Dataset looks pretty good. Minor issues. 

Does this dataset have all the attributes (columns) that we would like? 

The url of each title would be nice. Lets save it for later. 

## Next steps
Lets keep expanding the years
Next: [[Bollyset Scraper Part 4 - More Years]]
