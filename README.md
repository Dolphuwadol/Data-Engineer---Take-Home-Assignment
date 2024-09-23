# Data-Engineer---Take-Home-Assignment
## Question 1 - Data Pipeline Design
This project implements a scalable, robust data pipeline for processing both Master and Transactional data from MongoDB to BigQuery. It's designed to cater to users with various levels of data literacy, from business users with basic SQL knowledge to data scientists.

### Key Features

- Scalable design using GCP services
- Robust data quality checks and exception handling
- User-friendly data serving layer in BigQuery
- Support for both batch and potential streaming data ingestion

### Technology Stack
- Google Cloud Platform (GCP)
- MongoDB
- Apache Beam / Cloud Dataflow
- Cloud Storage
- BigQuery
- Cloud Composer (Apache Airflow)
- Data Catalog

### Serving Layer Data Management Guidelines
1. **Create Simplified Views for End Users**
   - Design views that present data in a simple, user-friendly format.
   - Ensure that complex structures (such as nested arrays) are flattened. For example, arrays should be unwrapped into separate rows, making the data easier to work with.

2. **Use Clear and Meaningful Column Names**

3. **Create Materialized Views for Frequent Calculations**
   - Identify common or frequently-used calculations and create materialized views to store the precomputed results.
   - Materialized views help improve query performance by reducing the need to perform repeated calculations.

4. **Develop Specialized Data Marts**
   - Build data marts for specific business functions or domains.
   - Each data mart should be tailored to the needs of specific user groups, providing focused access to the most relevant data.

5. **Implement Data Governance with a Data Catalog**
   - Use a data catalog to manage metadata, ensuring that information about data sources, structures, and transformations is easily accessible.
   - This will enable better discovery of data and help users locate the information they need quickly and efficiently.


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
   cd Text_sanitizer
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
```

## Question 3 - SQL
### Objective
Extract the product names and product classes for the top 2 sales for each product class in our product universe, ordered by class and then by sales. If there are any tie breakers, use the lower quantity to break the tie

### SQL Script Explanation
The SQL script retrieves and ranks products from the sales dataset. It calculates the sales value for each product by multiplying the retail price by the quantity sold. The results include the top two products by sales value for each product class.


