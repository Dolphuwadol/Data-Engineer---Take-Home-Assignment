# Data-Engineer---Take-Home-Assignment

# Question 1 - Data Pipeline Design
## Text Sanitizer Application

## Overview

The Text Sanitizer Application is a Python program designed to sanitize input text by cleaning and transforming it, and then calculating character statistics. The application supports multiple input sources such as files or databases (PostgreSQL). It can efficiently handle large files using memory-mapped IO, and it utilizes parallel processing to sanitize text and calculate statistics in chunks for performance optimization.

## Features

- **Multiple Input Sources**:
  - Read from a file using memory-mapped IO (efficient for large files).
  - Read from a PostgreSQL database by executing SQL queries.
  
- **Text Sanitization**:
  - Converts text to lowercase.
  - Replaces certain special characters (e.g., tabs replaced with underscores).
  
- **Statistics Calculation**:
  - Calculates alphabet statistics (frequency of each letter) in the sanitized text.
  
- **Output**:
  - Writes the sanitized text and calculated statistics to an output file.
  
- **Configurable**:
  - Command-line arguments or configuration file-based setup for flexibility.
  
- **Parallel Processing**:
  - Uses multiprocessing to sanitize text and compute statistics in parallel, improving performance on large datasets.

## Requirements
- Python 3.8+
- PostgreSQL (if using database input)


