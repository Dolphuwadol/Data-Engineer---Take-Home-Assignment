# Data-Engineer---Take-Home-Assignment

# Question 1 - Data Pipeline Design
## Text Sanitizer Application

## Overview

The Text Sanitizer Application is a program designed to sanitize text and calculate character statistics, supporting input from files and PostgreSQL databases.

## Features
- Read from files or PostgreSQL databases.
- Sanitize text (convert to lowercase, replace tabs).
- Calculate alphabet frequency statistics.
- Output results to a file.
- Uses multiprocessing to sanitize text and compute statistics in parallel, improving performance on large datasets.


## Requirements
- Python 3.8+
- PostgreSQL (if using database input)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Dolphuwadol/Data-Engineer---Take-Home-Assignment.git
   cd text-sanitizer-app
2. Install required packages:
   ```bash
    pip install psycopg2

## Usage
Command-Line Example
1. For read a text file from "Source" and write to "Target"
   ```bash
    python text_sanitizer.py --source file --input input.txt --target output.txt

2. For read a text file from "Database" and write to "Target"
   ```bash
    python text_sanitizer.py --source db --query "select name from public.EVENT$EVENTPLANNING" --target output.txt --config config.ini

Config File Example (config.ini)
   ```ini
    [settings]
  source = db
  query = SELECT text_column FROM your_table
  target = output.txt
  
  [database]
  dbname = your_db
  user = your_user
  password = your_password
  host = localhost
  port = 5432
  ```
Run with config:
   ```bash
      python text_sanitizer.py --config config.ini 




 


  




