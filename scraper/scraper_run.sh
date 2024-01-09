#!/bin/bash

conda activate bollyset

cd ./holistic_llm_scraper
scrapy crawl gnm_spider
