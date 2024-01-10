#!/bin/bash

conda activate bollyset

cd ./bollywiki_scraper
scrapy crawl bollywiki_spider
