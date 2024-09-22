# Data-Engineer---Take-Home-Assignment

## Question 2 - Text Sanitizer
### Overview

The Text Sanitizer Application is a program designed to sanitize text and calculate character statistics, supporting input from files and PostgreSQL databases.

### Features
- Read from files or PostgreSQL databases.
- Sanitize text (convert to lowercase, replace tabs).
- Calculate alphabet frequency statistics.
- Output results to a file.
- Uses multiprocessing to sanitize text and compute statistics in parallel, improving performance on large datasets.


### Requirements
- Python 3.8+
- PostgreSQL 

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Dolphuwadol/Data-Engineer---Take-Home-Assignment.git
   cd text-sanitizer-app
2. Install required packages:
   ```bash
    pip install psycopg2

### Usage
#### 1. Command-Line Example
1. For read a text file from "Source" and write to "Target"
   ```bash
    python text_sanitizer.py --source file --input input.txt --target output.txt

2. For read a text file from "Database" and write to "Target"
   ```bash
    python text_sanitizer.py --source db --query "select colums_name from table_name" --target output.txt --config config.ini

#### 2. Config File Example (`config.ini`)
1. For File Input
```ini
[settings]
source = file
input = input.txt
target = output.txt
```

2. For Database Input
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
