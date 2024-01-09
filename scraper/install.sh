#!/bin/bash
conda init

# Define the name of the Conda environment
conda_env_name="bollyset"

# Create Conda environment from the environment.yml file
conda env create -f environment.yml

# Activate the Conda environment
conda activate $conda_env_name

# playwright not needed in this project yet

# Install additional Python packages using pip
# pip install scrapy-playwright

# Initialize Playwright
# playwright install
