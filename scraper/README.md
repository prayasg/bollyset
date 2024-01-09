# Scraper for Holistic LLM

## Prerequisites
conda installed on your computer
basic knowledge of github, terminal

## Installing
1. Clone this repo
2. Give run permissions to the script and Run the install script
```
chmod +x ./install.sh
bash ./install.sh
```
3. Activate the environment
```
conda activate bollyset
```

## Running the scraper: manual run 
1. CD to the scraper directory and Run the scraper
```
cd ./holistic_llm_scraper
scrapy crawl gnm_spider
```

## Running the scraper: automated run
1. Give run permissions to the script and Run the install script
```
chmod +x ./scraper_run.sh
```
2. run the script
```
bash ./scraper_run.sh
```

## One-time setup to create scrapy project (already complete)
0. CD into scraper directory
```
cd scraper
```
1. Create a scrapy project
```
scrapy startproject holistic_llm_scraper
```
2. Create a spider
```
cd holistic_llm_scraper
scrapy genspider bolly_wiki_scraper https://en.wikipedia.org/wiki/Lists_of_Hindi_films
```