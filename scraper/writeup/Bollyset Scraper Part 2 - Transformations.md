Parts:
- [[Bollyset Scraper Part 1 - Getting Started]]
- [[Bollyset Scraper Part 2 - Transformations]]
- [[Bollyset Scraper Part 3 - 2023 and csv]]
- [[Bollyset Scraper Part 4 - More Years]]
- [[Bollyset Scraper Part 5 - 2017 table version]]
- [[Bollyset Scraper Part 6 - 2007 and prior]]
## Code so far
```
import scrapy
import pandas as pd
from io import StringIO

class BollywikiSpiderSpider(scrapy.Spider):
    name = "bollywiki_spider"
    allowed_domains = ["en.wikipedia.org"]

    # urls = pd.read_csv('data/urls.csv')
    # start_urls = urls['start_urls'].tolist()
    start_urls = ['https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024']
    
    def parse(self, response):
        self.logger.info(f"response: {response.url}")
        
        tables = response.xpath('//table[@class="wikitable"]').getall()
        self.logger.info(f"tables: {len(tables)}")

        for table in tables:
            for t in pd.read_html(StringIO(table)):
                t.to_csv('data/sample_table.csv', index=False)
                break
            break

class BollywikiScraperItem(scrapy.Item):
    title = scrapy.Field()

```

## The issue of the table
The table is misbehaving. Whats the best way to proceed? 
- pandas read_html produces a good table, but loses html tags - forget the tags for now
- many different ways to structure the item
- what data do i want? some is hard to get
- manually parsing the html feels like a loosing game

Decision: use read_html, loose the tags. 
`sample_table.csv`

| Opening  | Opening.1 | Title             | Director          | Cast                                  | Studio (production house)             | Ref. |
|----------|-----------|-------------------|-------------------|---------------------------------------|--------------------------------------|------|
| J A N    | 5         | Tauba Tera Jalwa  | Akashaditya Lama  | Jatin KhuranaAmeesha PatelAngela Krislinzki | Shreeram Productions, Victorious Enterprises | [1]  |
| J A N    | 12        | Merry Christmas[a] | Sriram Raghavan   | Katrina KaifVijay Sethupathi          | Tips Industries, Matchbox Pictures   | [2]  |

Notes:
- Column names need to be normalized
- Dates are weird
- there is an `[a]` tag 
- Cast values are f'd
- ref doesnt go anywhere

## data cleanup

interactively use pandas to build a sequence of transformations on an input table. start the first step, look at the results, keep adjusting until satisfied. 

round 1 - add a processing function
```
    def parse(self, response):
        self.logger.info(f"response: {response.url}")
        
        tables = response.xpath('//table[@class="wikitable"]') #.getall() selector -> str
        self.logger.info(f"tables: {len(tables)}")

        for table in tables:
			table = table.get() # selector to string
            item = self.process_table(table)
            yield item

    def process_table(self, table):
        item = BollywikiScraperItem()
        self.logger.info(f"table: {table}")
        return item

```

Doesnt work?

## too many logs
hard to figure out whats happening from the logs. Lets dial down the noise

scrapy has built in [logging](https://docs.scrapy.org/en/latest/topics/logging.html) (read the whole thing)

many different loggers: `scrapy`, `scrapy.core`, `bollywiki_spider` and more. 

how do we fix this?
- tried to google didnt get anywhere
- chatgpt gave solution

use `self.logger.warning` with the terminal command `scrapy crawl bollywiki_spider -L WARNING`

## Transformations

### round 1 - add a process function
```
    def parse(self, response):
        self.logger.warning(f"response: {response.url}")
        
        tables = response.xpath('//table[@class="wikitable"]')
        # tables is a list of selectors
        self.logger.warning(f"# tables: {len(tables)}")
        self.logger.warning(f"{tables}")
	    
        for table in tables:
            item = self.process_table(table) 
            yield item

    def process_table(self, table):
        table = table.get() # selector to string
        self.logger.warning(f"table: {table}")
        
        item = BollywikiScraperItem()
        return item

```

this function will house all the logic for the transformation. 

transformation means blob of html -> structured datum.
scrapy provides many selectors (xpath, css, etc). to isolate pieces of html
must parse that html to pick out the data needed
## html structure

lets talk about the html table. use google chrome inspect to see the html tag structure. there is some structure. lots of landmines. i have stepped on a bunch and have filtered them out. your success will depend on your ability to naviagate forward dispite those landmines

```
 <table>
	 <tbody> 
		 <tr> # first row header
		 ...  
		 <tr> # one title per row
			 <td> # html contents of cell. high variety
```

a couple approaches:
#### Manually parse
```
item = {}
tables = response.xpath('//table[@class="wikitable"]')
table = tables[0]

trs = table.xpath('//tbody//tr)
for tr in trs:
	tds = tr.xpath('.//td')
	for td in tds:
		if td.class == 'Title'
		item['title'] = td.value
```

#### use the html table parser from pandas
```
import pandas as pd

item = {}
tables = response.xpath('//table[@class="wikitable"]')
table = tables[0]

dfs = pd.read_html(table) 
# returns list of dfs
df = dfs[0]
```

#### Comparison

Overall: pandas parser
pros: 
- works out of the box. convenient
- gets most of the data
cons: 
- missing some important data

to get that important data, do manual. later. too much work. 

## Transformations
Notes from earlier:
- Column names need to be normalized
- Dates are weird
- keep title, director, cast, studio
- there is an `[a]` suffix on some titles needs to be cleaned 
- Cast values are f'd
- ref doesnt go anywhere

### Round 2 - add pandas parsing
```
    def process_table(self, table):
        table = table.get() # selector to string
        df = pd.read_html(table)[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        item = BollywikiScraperItem()
        return item
```

### Round 3 - change column names
```
    def process_table(self, table):
        table = table.get() # selector to string
        df = pd.read_html(table)[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        # transformations
        # round 3
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        item = BollywikiScraperItem()
        return item

```

### Round 4 - get title
```
    def process_table(self, table):
        table = table.get() # selector to string
        df = pd.read_html(table)[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        # transformations
        # round 3
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        for i, row in df.iterrows():
            item = BollywikiScraperItem()
            item['title'] = row['title']
            yield item

        return item
```

### Round 5 - get director
start with director list. update item and function

```

class BollywikiScraperItem(scrapy.Item):
    title = scrapy.Field()
    director_list = scrapy.Field()

def process_table(self, table):
	table = table.get() # selector to string
	df = pd.read_html(table)[0] # string to dataframe
	self.logger.warning(f"table: {df}")

	# transformations
	# round 3
	column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
	df.columns = column_names

	for i, row in df.iterrows():
		item = BollywikiScraperItem()
		# round 4
		item['title'] = row['title']
		# round 5
		item['director'] = row['director']

		self.logger.warning(f"item: {item}")
		yield item

```

good so far.

### Round 6 - get cast
```

    def process_table(self, table):
        table = table.get() # selector to string
        df = pd.read_html(table)[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        # transformations
        # round 3
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        for i, row in df.iterrows():
            item = BollywikiScraperItem()
            # round 4
            item['title'] = row['title']
            # round 5
            item['director'] = row['director']


            self.logger.warning(f"item: {item}")
            yield item
```
here is an example item:

```
{'cast': 'Ravi KishanNitanshi GoelPratibha RantaSparsh ShrivastavaChhaya Kadam',
 'director': 'Kiran Rao',
 'title': 'Laapataa Ladies'}
```

the cast is f'd. How can we fix this?
it should look like `'Ravi Kishan, Nitanshi Goel, Pratibha Ranta, Sparsh Shrivastava, Chhaya Kadam'`

This took me a while to figure out. Had to google for a while, took a while to formulate the question. had to dive into the html. eventually chatgpt pulled through. 


## Fixing F'd cast values
an example of one of the many tedious things youll have to do to find bits of data in the internet wilds


chatgpt prompt:
```
I am using pandas.read_html to parse an HTML table. The HTML has data in an unordered list. the read_html function is appending all the values together. How do I comma-seperate them?
```

gave a pretty good explaination + code sample 

code sample:
```
import pandas as pd
from bs4 import BeautifulSoup

# Assuming 'html_content' contains your HTML data

# Parse the HTML with BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

# Find all unordered lists
unordered_lists = soup.find_all('ul')

for ul in unordered_lists:
    # Extract text from each list item and join them with a comma
    text = ', '.join(li.get_text() for li in ul.find_all('li'))
    # Replace the ul contents with the comma-separated text
    ul.string = text

# Now the HTML is modified with comma-separated values in place of lists
# Convert it back to a string and use pandas.read_html
modified_html = str(soup)
dfs = pd.read_html(modified_html)

# 'dfs' is a list of dataframes obtained from the HTML tables

```

The pandas parser has a specific rule baked in about what to do when the table cell has a set of unordered items:
- `glue them all togther: [<ul> Thing, <ul> Item, Other] -> ThingItemOther`

This works by modifying the html itself before the pandas parser sees it. Changing the `ul` to `li` (ordered list)

Integrate this into the parser by creating a saparte function and calling it in `process_table`

```

    def process_table(self, table):
        table = self.preprocess_html(table) # fix ul issue
        df = pd.read_html(table)[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        # transformations
        # round 3
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        for i, row in df.iterrows():
            item = BollywikiScraperItem()
            # round 4
            item['title'] = row['title']
            # round 5
            item['director'] = row['director']
            # round 6
            item['cast'] = row['cast']


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

item: 
```
{'cast': 'Abhay Verma,Sharvari Wagh,Mona Singh',
 'director': 'Aditya Sarpotdar',
 'title': 'Munjhya'}
```

## Are we done yet?
What signifies done?

Desired dataset:
- movie
- director
- producer
- cast
- date released

Would be nice: 
- reception
- profit
- genre
- plot summary 

Lets add date and be done. 

### Round 7 - add date
The date parts are scattered:
- year is in the url we are scraping
- month is in the first column
- date in the second column

add each of these as a column to the df, let pandas convert to date.

we can get year from the url
```
df['year'] = response.url.split('_')[-1]
```

the table we are parsing currently looks like this:
```
df.loc[:, ['opening_month', 'opening_day', 'year']] # select only these cols
```

which looks like this: 

| opening_month | opening_day | year  |
|---------------|-------------|-------|
| J A N         | 5           | 2024  |
| J A N         | 12          | 2024  |
| J A N         | 19          | 2024  |
| J A N         | 19          | 2024  |
| J A N         | 25          | 2024  |

need to fix up the month col
```
# J A N -> JAN
df.opening_month.str.replace(' ', '') 
```

constructing the full date
```
# | J A N | 5 | 2024 | -> [JAN, 5, 2024]
df.loc[:, ['opening_month', 'opening_day', 'year']].apply(lambda x: x.tolist(), axis=1) 

# | J A N | 5 | 2024 | -> JAN,5,2024
df.loc[:, ['opening_month', 'opening_day', 'year']].apply(lambda x: ','.join(x.tolist()), axis=1)

```

final code 
```
df['date'] = df.loc[:, ['opening_month', 'opening_day', 'year']].apply(lambda x: ','.join(x.tolist()), axis=1)

df['date'] = pd.to_datetime(df['date'], format='%b,%d,%Y')
# 2024-01-05
```

spider parse function
```

    def process_table(self, table, response):
        table = self.preprocess_html(table) # fix ul issue
        df = pd.read_html(StringIO(table))[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        # transformations
        # round 3
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        # round 7
        df['year'] = response.url.split('_')[-1] 
        df['opening_day'] = df['opening_day'].apply(str)
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

```

example result
```
{'cast': 'Abhay Verma,Sharvari Wagh,Mona Singh',
 'director': 'Aditya Sarpotdar',
 'opening_date': '2024-03-29',
 'opening_year': '2024',
 'title': 'Munjhya'}
```

## full 2024
try to run the full page and... more errors! more transformations needed

### round 8 - fix nulls
from the error we see some tables have `NULL` values. 

replace `NULL` with an empty string
```
    def process_table(self, table, response):
        table = self.preprocess_html(table) # fix ul issue
        df = pd.read_html(StringIO(table))[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        # transformations
        # round 3
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        # round 8
        df = df.fillna('')
```

more errors! sometimes the day is missing too and `"JUL,,2024"` isnt a valid date

```
# instead of...
df = df.fillna('')

# do
subset = ['title', 'director', 'cast', 'studio', 'ref']
df.loc[:, subset] = df.loc[:, subset].fillna('')

subset = ['opening_month', 'opening_day']
df.loc[:, subset] = df.loc[:, subset].fillna('1')
```

found some more bugs while doing this. `NULL`s can really mess up things. 
## Final look
Okay so the scraper runs successfully and the dicts are all populated. 

Is the data correct though?

Modify the spider to save the output items as a csv. Eventually we will bake this as a pipeline that would output directly to a local file, then eventually to a database. 



## 2024 script
We have successfully scraped the 2024 page and produced a dataset.

The spider so far
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



```

## Next steps

- try out on 2023 and beyond!

next step: [[Bollyset Scraper Part 3 - 2023 and csv]]
