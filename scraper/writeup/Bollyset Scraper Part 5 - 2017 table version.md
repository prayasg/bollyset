Parts:
- [[Bollyset Scraper Part 1 - Getting Started]]
- [[Bollyset Scraper Part 2 - Transformations]]
- [[Bollyset Scraper Part 3 - 2023 and csv]]
- [[Bollyset Scraper Part 4 - More Years]]
- [[Bollyset Scraper Part 5 - 2017 table version]]
- [[Bollyset Scraper Part 6 - 2007 and prior]]
## Story so far
Our scaper is successfully pulling data for the years 2018 - 2024.

[2017](https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2017) adds the `genre` column.

This column exists in all tables up until 2008.

**Notable observation:** `genre` is a list of values. The list sepraters vary between `,` and  `/`

Our solution will need to adjust for that. 

## New column new architecture new me
lets think about how we move forward. 

The code we have so far
```

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
```

hopefully you can see that `parse` and `preprocess_html` dont need to change. 
Confirmed that parse will continue to work by making sure the tables in 2013 look like `<table class"wikitable">`

Lost by that last sentence? You've come too far. Go back. Go get some help. 
You following? Lets keep going. 

## Process Table Architecture
This method's logic is roughly as follows:
- convert html text to df
- if df is the wrong shape, exit
- change column names
- df transformations
	- handle missing values, based on column names
	- fix the date
- create an item for each row

Most of this is doesnt need to change. But how we think about our task needs to change.

Current way: The wiki tables always have all the columns im looking for. I can assume a specific dataframe model and work with that. 

New way: There is a standard data structure. Different years will have different subsets of that overall data structure. Regardless of the year, the same data structure should be output. 

## New architecture in detail
New method logic flow: 
- parse table
	- convert html text to df
	- if df is the wrong shape, exit
	- if the year is between 2018-2024
		- figure that column situation out
		- create df
	- if the year is between 2012-2017
		- figure that column situation out
		- create df
	- do the rest of the df transformations as above

Try doing this on your own. Apply chatgpt liberally. Get your grade first then look at the suggested script solution. 

Grade C: you get some of the data from 2008-2017 to come through
Grade B: you get all the data 2008 - 2024 to come through, your code is ugly
Grade A: you get all the data 2008 - 2024 to come through, your code is not ugly
Grade A+: you get all the data 2008 - 2024 to come through, your code is better than my code

## Testing - whats my grade?
How do you know if what you did is right or not? Its probably not. Whats wrong with it?

A non-exhaustive list of checks:
- go through each year one by one, make sure each one works and that data looks normal. 
- run the whole dataset. is it weird or blank? 
	- Tableau is useful for this.
	- excel is okay

### CSV output
Saved as `bollyset_2012_2024.csv`

### Total Item counts
Item counts by year and total:
| Opening Year | Count |
|--------------|-------|
| 2008         | 87    |
| 2009         | 101   |
| 2010         | 124   |
| 2011         | 107   |
| 2012         | 104   |
| 2013         | 122   |
| 2014         | 138   |
| 2015         | 112   |
| 2016         | 124   |
| 2017         | 126   |
| 2018         | 103   |
| 2019         | 127   |
| 2020         | 103   |
| 2021         | 103   |
| 2022         | 106   |
| 2023         | 135   |
| 2024         | 34    |
| Grand Total  | 1,856 |

## Suggested Implementation

Rearchitect the parse function to the following logic: 
- load df, check min column size
- rename the columns that are always present
- go looking for certain column using known names, add empty column if not found
- do the rest of the transformations

Generally, put groups of logic in their own methods to keep some organization. 
### clean up `process_table`
```
  
  

def process_table(self, table, response):

table = self.preprocess_html(table) # fix ul issue

  

df = pd.read_html(StringIO(table))[0] # string to dataframe

self.logger.warning(f"df shape: {df.shape}") # rows, columns

self.logger.warning(f"df columns: {df.columns}") # column names

self.logger.info(f"table: {df}") # at a lower level, there if you want to see it default hidden

  
  

# basic transformations

year = response.url.split('_')[-1]

df['year'] = year

  

if self.df_skip(df):

return

  

# Filter rows with more than 5 null values

# sometimes the table picks up ghost rows at the bottom

df = df[df.isnull().sum(axis=1) <= 3]

  

# Filter rows where the string length is more than 6

# bug in the wiki data. one table has a row with a long string

# Should be "J U N" but is "Good Luck Jerry"

df = df[df['Opening'].str.len() <= 6]

# df = df[df['Opening.1'].str.len() <= 4]

# self.logger.warning(f"df opening: {df['Opening.1'].unique()}") # rows, columns

  

df = self.standardize_column_names(df)

df = self.df_transformations(df)

  

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

```


### new method: `df_skip`
```

def df_skip(self, df):
	# skip if table has too few rows
	if df.shape[0] < 2:
		self.logger.warning(f"df shape: {df.shape}. too few rows. skipping.")
		return True

	# skip if table has columns with the work "Rank" or "Gross"
	if 'Rank' in df.columns: 
		self.logger.warning(f"df columns: {df.columns}. has Rank or Gross. skipping.")
		return True
	
	if any('Gross' in col for col in df.columns):
		self.logger.warning(f"df columns: {df.columns}. has Rank or Gross. skipping.")
		return True
	
	return False
```
### new method: `standardize_column_names`:
```


def standardize_column_names(self, df):
	common_column_names = {
		'Opening':'opening_month',
		'Opening.1':'opening_day',
		'Title':'title',
		'Director':'director',
		'Cast':'cast',
	}
	df = df.rename(columns=common_column_names)

	columns = df.columns.tolist()

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

```


### new method: `df_transformations`
this collects all the transformations done to the standardized df
note, genre has been added
- sometimes, the genres are split by `/`. make sure its always `,`
- make all values lower case to standardize

```

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
	df['opening_month'] = df['opening_month'].str.replace('[0-9]+', '', regex=True)
	df = df.applymap(lambda x: None if x == '' else x)
	# filter out rows where opening_month is null
	df = df[df['opening_month'].notnull()]

	dates = df.loc[:, ['opening_month', 'opening_day', 'year']].copy() # ['JAN', '1', '2024']
	dates = dates.apply(lambda x: x.tolist(), axis=1) # ['JAN','1','2024']
	dates = dates.apply(lambda x: ','.join(x)) # 'JAN,1,2024'

	dates = pd.to_datetime(dates, format='%b,%d,%Y') # pandas date object
	df['date'] = dates.dt.strftime('%Y-%m-%d') # date obj to string

	df['genre'] = df['genre'].str.replace('/', ',')
	df['genre'] = df['genre'].str.lower()

	return df
```

## the whole spider

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
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2016',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2015',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2014',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2013',
        'https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2012',
    ]

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


        # basic transformations
        year = response.url.split('_')[-1] 
        df['year'] = year

        if self.df_skip(df):
            return

        # Filter rows with more than 5 null values
        # sometimes the table picks up ghost rows at the bottom
        df = df[df.isnull().sum(axis=1) <= 3]

        # Filter rows where the string length is more than 6
        # bug in the wiki data. one table has a row with a long string
        # Should be "J U N" but is "Good Luck Jerry"
        df = df[df['Opening'].str.len() <= 6]
        # df = df[df['Opening.1'].str.len() <= 4]
        # self.logger.warning(f"df opening: {df['Opening.1'].unique()}")  # rows, columns

        df = self.standardize_column_names(df)
        df = self.df_transformations(df)

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
        # skip if table has too few columns
        if df.shape[1] < 6:
            self.logger.warning(f"df shape: {df.shape}. too few columns. skipping.")
            return True
        
        # skip if table has too few rows
        if df.shape[0] < 2:
            self.logger.warning(f"df shape: {df.shape}. too few rows. skipping.")
            return True

        # skip if table has columns with the work "Rank" or "Gross"
        if 'Rank' in df.columns: 
            self.logger.warning(f"df columns: {df.columns}. has Rank or Gross. skipping.")
            return True
        
        if any('Gross' in col for col in df.columns):
            self.logger.warning(f"df columns: {df.columns}. has Rank or Gross. skipping.")
            return True
        
        return False


    def standardize_column_names(self, df):
        common_column_names = {
            'Opening':'opening_month',
            'Opening.1':'opening_day',
            'Title':'title',
            'Director':'director',
            'Cast':'cast',
        }
        df = df.rename(columns=common_column_names)

        columns = df.columns.tolist()

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
        df['opening_month'] = df['opening_month'].str.replace('[0-9]+', '', regex=True)
        df = df.applymap(lambda x: None if x == '' else x)
        # filter out rows where opening_month is null
        df = df[df['opening_month'].notnull()]

        dates = df.loc[:, ['opening_month', 'opening_day', 'year']].copy() # ['JAN', '1', '2024']
        dates = dates.apply(lambda x: x.tolist(), axis=1) # ['JAN','1','2024']
        dates = dates.apply(lambda x: ','.join(x)) # 'JAN,1,2024'

        dates = pd.to_datetime(dates, format='%b,%d,%Y') # pandas date object
        df['date'] = dates.dt.strftime('%Y-%m-%d') # date obj to string

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
```


## Next steps
Works up till 2007. Going strong! Lets keep going.
Next part [[Bollyset Scraper Part 6 - 2007 and prior]]
