Parts:
- [[Bollyset Scraper Part 1 - Getting Started]]
- [[Bollyset Scraper Part 2 - Transformations]]
- [[Bollyset Scraper Part 3 - 2023 and csv]]
- [[Bollyset Scraper Part 4 - More Years]]
- [[Bollyset Scraper Part 5 - 2017 table version]]
- [[Bollyset Scraper Part 6 - 2007 and prior]]
## your first spider

create the scrapy project and the first spider.

```

conda activate bollyset
scrapy startproject bollywiki_scraper
cd bollywiki_scraper

scrapy genspider bollywiki_spider https://en.wikipedia.org/wiki/Lists_of_Hindi_films
```

code for the spider
```
import scrapy

class BollywikiSpiderSpider(scrapy.Spider):
    name = "bollywiki_spider"
    allowed_domains = ["en.wikipedia.org"]
    start_urls = ["https://en.wikipedia.org/wiki/Lists_of_Hindi_films"]

    def parse(self, response):
        self.logger.info("A response from %s just arrived!", response.url)

```

## running the spider
To run the spider in your terminal:
```
scrapy crawl bollywiki_spider
```

## enhancing the spider

start with [docs for spider](https://docs.scrapy.org/en/latest/topics/spiders.html)
the important thing here is the parse method. lets play with that.

```
    def parse(self, response):
        self.logger.info("A response from %s just arrived!", response.url)
```

this prints out one line, the one start_url value. 

try this
```
    def parse(self, response):
        self.logger.info("A response from %s just arrived!", response.url)
        for href in response.xpath("//a/@href").getall():
			self.logger.info("Found href: %s", href)
```

This finds a lot of links. All the links from the main article. Also, all the links from the tables in purple that are collapsed. Lists of movies by language, continent, and country. 

Related, there is [this link](https://en.wikipedia.org/wiki/Category:Hindi-language_films) that I didnt go deeper into. 

So, lets filter to just hindi. [Selectors](https://docs.scrapy.org/en/latest/topics/selectors.html#working-with-xpaths) is helpful. Running this in shell mode.
```
# extract all links in the response
hrefs = response.xpath("//ul//a/@href").getall()

# no filter
len(hrefs) # no filter 1121

# filter: the url contains that string
hrefs = [href for href in hrefs if '/wiki/List_of_Hindi_films_of' in href]
len(hrefs) # 210
hrefs
```

Our parse function looks like this now
```

    def parse(self, response):
        self.logger.info(f"response: {response.url}")
        
        for href in response.xpath("//ul//a/@href").getall():
            if '/wiki/List_of_Hindi_films_of' in href:
                yield scrapy.Request(response.urljoin(href), self.parse)

```

## wiki structure
Spend some time understanding how [the webpage](https://en.wikipedia.org/wiki/Lists_of_Hindi_films) is structured. 

We care about the [List of Hindi films of 2023](https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2023) types of links. Used this as the filter. 

A few interesting things about this page:
- this page has the urls to all the other lists in the purple box on the top left. this is a problem.
- there are multiple tables. the first is `highest worldwide gross`, then a table for each season `Jan - Mar`, `Apr - June` etc 
- There is a variety of information
	- the date (complicated)
	- Title (text or url)
	- Director (text or url)
	- Cast (Text or url)
	- Studio (Text or url)
	- Ref (external urls)

We must make some decisions. What is the optimal spider structure? What data do we want out of this page? How should we store it?

Spiders are called recursively. Spiders must return requests carefully, or they could get caught in loops. Remember: each list page has a list to all url pages, including itself. 

There are some urls we will want to follow, and some we dont. We will build this spider system in pieces, for example processing the url for a movie differently than the url for an actor. 

Decisions: 
- We will not include follow up requests in parse. 
- Each request will only produce Items for the data we want to collect the data: date, title, director, etc. We want both the text and the url. 
-  `start_urls` will be provided as a list, created offline (KISS)
- data will be stored locally as a csv 

## start urls
using the same scrapy shell earlier when we created the filter. 
```
import pandas as pd

urls = [response.urljoin(href) for href in hrefs]
urls = pd.Series(urls, name='start_urls')
urls.to_csv('data/urls.csv', index=False)
```

new spider code:
```
import scrapy
import pandas as pd

class BollywikiSpiderSpider(scrapy.Spider):
    name = "bollywiki_spider"
    allowed_domains = ["en.wikipedia.org"]

    urls = pd.read_csv('data/urls.csv')
    start_urls = urls['start_urls'].tolist()
    
    def parse(self, response):
        self.logger.info(f"response: {response.url}")

        # hrefs = response.xpath("//ul//a/@href").getall()
        # hrefs = [href for href in hrefs if '/wiki/List_of_Hindi_films_of' in href]
        # len(hrefs)
        # hrefs
        
        # for href in response.xpath("//ul//a/@href").getall():
            # if '/wiki/List_of_Hindi_films_of' in href:
                # yield scrapy.Request(response.urljoin(href), self.parse)
```

- [ ] Todo: integrate this into the spider somehow. 
- [ ] Todo: Expansion to the other languages. 
## Parsing a list page
Focus on [2024](https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024). This page is slightly different than 2023. We want each of the tables on this page. 

The spider:
```
import scrapy
import pandas as pd

class BollywikiSpiderSpider(scrapy.Spider):
    name = "bollywiki_spider"
    allowed_domains = ["en.wikipedia.org"]

    # urls = pd.read_csv('data/urls.csv')
    # start_urls = urls['start_urls'].tolist()
    start_urls = ['https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024']
    
    def parse(self, response):
        self.logger.info(f"response: {response.url}")

```

Start with the shell again
```
scrapy shell https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024
```

Using Chrome Inspect on that url, we see each table we want has this tag: `<table class="wikitable">`
```
tables = response.xpath('//table[@class="wikitable"]').getall()
len(tables) # 4, looks good

for table in tables:
	for t in pd.read_html(table):
		t.to_csv('data/sample_table.csv', index=False)
		break
	break
```

Notes from the csv:
- date is weird, as expected
- Title is just text. What about URL?
- Cast has bad grammer

This sucks. We are missing some important data. Making our own html table parser. Good time to introduce items.

Current spider
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

```

## Items and Loaders
Spider's parse will produce [Items](https://docs.scrapy.org/en/latest/topics/items.html). This is basically a fancy dict. Next, learn about [Item Loaders](https://docs.scrapy.org/en/latest/topics/loaders.html). Helps create items. [Selectors](https://itemloaders.readthedocs.io/en/latest/built-in-processors.html#built-in-processors), an important aspect of loaders. 

Read through the pages above. Yes, all of the pages. 

### Item
Items will define the structure of the spider output. Comparable to defining the columns for the output dataset we want. 

Looking through numerous list wiki pages:
- the format of the tables has changed over the years
- Values can be only text or the text with url. 
- Sometimes import data (like the year) is somewhere outside of the table. 

Lets start with the minimum. Each item will be one Movie. We will get the title and if it exists, the url. 
```
import scrapy

class BollywikiScraperItem(scrapy.Item):
    title = scrapy.Field()
    title_link = scrapy.Field()
```

### Loader
Need to make some more decisions. We know there are multiple table formats. Lets just start with 2024. 

I got stuck here. Complexity growing rapidly. Help. Taking a break.

Check into github


Next part: [[Bollyset Scraper Part 2 - Transformations]]

## Shell / Debugging
[Debugging general](https://docs.scrapy.org/en/latest/topics/debug.html)
[Shell docs](https://docs.scrapy.org/en/latest/topics/shell.html#launch-the-shell)

```
scrapy shell https://en.wikipedia.org/wiki/Lists_of_Hindi_films
```
