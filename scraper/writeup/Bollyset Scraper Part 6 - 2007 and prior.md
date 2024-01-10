Parts:
- [[Bollyset Scraper Part 1 - Getting Started]]
- [[Bollyset Scraper Part 2 - Transformations]]
- [[Bollyset Scraper Part 3 - 2023 and csv]]
- [[Bollyset Scraper Part 4 - More Years]]
- [[Bollyset Scraper Part 5 - 2017 table version]]
- [[Bollyset Scraper Part 6 - 2007 and prior]]
## 2007 and prior format
The format in the [2007 list](https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2007) is similar. Producer has fallen off. 

There are some issues with the opening date. The code was hardcoded to assume the date is there. 

Changed some stuff around in the scraper so that it would be robust to the date not being there. 

Had to add more logic to  `do_skip` to see if it is a rank page

Annnnnd were done with version 1!

Time check - took about 12 hours from the initiation of the project

## Final counts
| Decade        | Count |
|---------------|-------|
| 1920's        | 732   |
| 1930's        | 980   |
| 1940's        | 1,094 |
| 1950's        | 1,003 |
| 1960's        | 815   |
| 1970's        | 874   |
| 1980's        | 1,218 |
| 1990's        | 1,021 |
| 2000's        | 924   |
| 2010's        | 1,187 |
| 2020's        | 482   |
| Grand Total   | 10,330|
## Next steps
Whats next? 
- analysis of this data
	- standardizing names
	- standardizing genres
- looking at trends in actors and genres

Expanding scope of data
- getting data about titles specifically would be very interesting. we need the urls
- 