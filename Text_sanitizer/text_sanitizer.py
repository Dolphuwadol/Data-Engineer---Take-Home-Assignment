import argparse
import abc
from collections import Counter
from typing import Dict, Iterator, List
import io
import mmap
import multiprocessing
import os
import cProfile
import pstats
import io 
from functools import partial

import psycopg2
from psycopg2 import sql

import configparser

# Abstract base class for input reading
class InputReader(abc.ABC):
    @abc.abstractmethod
    def read(self) -> Iterator[str]:
        """Abstract method for reading input data"""
        pass

# Abstract base class for output writing
class OutputWriter(abc.ABC):
    @abc.abstractmethod
    def write(self, sanitized_text: Iterator[str], statistics: Dict[str, int]):
        """Abstract method for writing output data along with statistics"""
        pass

# Abstract base class for text sanitization
class TextSanitizer(abc.ABC):
    @abc.abstractmethod
    def sanitize(self, text: str) -> str:
        """Abstract method for sanitizing text"""
        pass

# Abstract base class for statistics calculation
class StatisticsCalculator(abc.ABC):
    @abc.abstractmethod
    def calculate(self, text: Iterator[str]) -> Dict[str, int]:
        """Abstract method for calculating statistics from the text"""
        pass

# File input reader using memory-mapped file for large files
class MemoryMappedFileInputReader(InputReader):
    def __init__(self, source: str, chunk_size: int = 1024 * 1024):  # 1 MB chunks
        """
        Initializes the memory-mapped file reader.
        :param source: Path to the input file
        :param chunk_size: Size of the chunks to read from the file (default 1MB)
        """
        self.source = source
        self.chunk_size = chunk_size

    # Use mmap to read the file in chunks for efficient memory usage
    def read(self) -> Iterator[str]:
        """Reads the input file using memory-mapped IO in chunks"""
        with open(self.source, 'r') as file:
            with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mmap_obj:
                for i in range(0, len(mmap_obj), self.chunk_size):
                    yield mmap_obj[i:i + self.chunk_size].decode('utf-8')

# Database input reader for reading data from PostgreSQL
class DatabaseInputReader(InputReader):
    def __init__(self, connection_params: dict, query: str):
        """
        Initializes the database reader.
        :param connection_params: Connection parameters for PostgreSQL
        :param query: SQL query to retrieve the data
        """
        self.connection_params = connection_params
        self.query = query

    def read(self) -> Iterator[str]:
        """Executes the SQL query and yields rows from the database as strings"""
        try:
            connection = psycopg2.connect(**self.connection_params)
            cursor = connection.cursor()
            cursor.execute(self.query)

            # Yield each row as a space-joined string
            for row in cursor.fetchall():
                yield ' '.join(map(str, row))

            cursor.close()
            connection.close()
        except (Exception, psycopg2.Error) as error:
            print(f"Error while connecting to PostgreSQL {error}")
            raise

# File output writer to write sanitized text and statistics
class FileOutputWriter(OutputWriter):
    def __init__(self, target: str):
        """
        Initializes the file writer.
        :param target: Path to the output file
        """
        self.target = target

    def write(self, sanitized_text: Iterator[str], statistics: Dict[str, int]):
        """Writes sanitized text and alphabet count statistics to the output file"""
        with open(self.target, 'w') as file:
            # Write each sanitized chunk of text
            for chunk in sanitized_text:
                file.write(chunk)
                print(chunk, end='')

            # Write the calculated statistics
            file.write('\nCount of alphabelt:\n')
            print("\nCount of alphabelt:")

            for char, count in statistics.items():
                file.write(f'{char}: {count}\n')
                print(f'{char}: {count}')

# Process-based text sanitizer
class ProcessTextSanitizer(TextSanitizer):
    def __init__(self):
        """Initializes the sanitizer with a translation table for specific characters"""
        self.translation_table = str.maketrans({'\t': '____'})  # Replace tabs with underscores

    def sanitize(self, text: str) -> str:
        """Sanitizes the text by converting it to lowercase and replacing tabs"""
        return text.lower().translate(self.translation_table)

# Calculates the alphabet statistics for the sanitized text
class AlphabetStatisticsCalculator(StatisticsCalculator):
    def calculate(self, text: Iterator[str]) -> Dict[str, int]:
        """Calculates and returns the count of alphabetic characters in the text"""
        counter = Counter()
        for chunk in text:
            counter.update(char for char in chunk.lower() if char.isalpha())
        return dict(counter)

# Helper function to sanitize a chunk of text
def sanitize_chunk(chunk: str, sanitizer: TextSanitizer) -> str:
    """Applies the sanitization process to a chunk of text"""
    return sanitizer.sanitize(chunk)

# Helper function to calculate statistics for a chunk of text
def calculate_statistics_chunk(chunk: str) -> Counter:
    """Calculates alphabet statistics for a chunk of text"""
    return Counter(char for char in chunk.lower() if char.isalpha())

# Main text processor that coordinates reading, sanitizing, and writing
class TextProcessor:
    def __init__(self, input_reader: InputReader, output_writer: OutputWriter,
                 sanitizer: TextSanitizer, statistics_calculator: StatisticsCalculator):
        """
        Initializes the text processor with input reader, output writer, sanitizer, and statistics calculator.
        :param input_reader: Reader object for the input source
        :param output_writer: Writer object for the output destination
        :param sanitizer: Sanitizer object to clean the text
        :param statistics_calculator: Calculator object for computing statistics
        """
        self.input_reader = input_reader
        self.output_writer = output_writer
        self.sanitizer = sanitizer
        self.statistics_calculator = statistics_calculator

    def process(self):
        """Processes the input by sanitizing text and calculating statistics, then writes the results"""
        input_text = self.input_reader.read()

        # Multiprocessing for sanitizing text chunks
        with multiprocessing.Pool() as pool:
            sanitized_chunks = pool.imap(
                partial(sanitize_chunk, sanitizer=self.sanitizer),
                input_text,
                chunksize=10
            )
            # Calculate statistics in parallel
            statistics_chunks = pool.imap(calculate_statistics_chunk, sanitized_chunks, chunksize=10)

            # Combine statistics from all chunks
            combined_statistics = Counter()
            for stat_chunk in statistics_chunks:
                combined_statistics.update(stat_chunk)

        # Re-read input for output writing (or store sanitized data temporarily)
        input_text = self.input_reader.read()
        sanitized_iterator = (self.sanitizer.sanitize(chunk) for chunk in input_text)
        self.output_writer.write(sanitized_iterator, dict(combined_statistics))

# Main function for parsing arguments and running the text processor
def main():
    """
    Main entry point for the text sanitizer application.
    Parses command-line arguments or config file settings to set up and run the processor.
    """
    parser = argparse.ArgumentParser(description="Text Sanitizer Application")
    parser.add_argument("--source", help="Source type (file or db)", required=False)
    parser.add_argument("--input", help="Input file path (required if source is 'file')", required=False)
    parser.add_argument("--target", help="Target file path", required=False)
    parser.add_argument("--query", help='SQL query reading from database', default=None)
    parser.add_argument("--config", help="Path to the config file", default=None)
    args = parser.parse_args()

    # Read config file if provided, fallback to command-line args if missing
    config = configparser.ConfigParser()
    connection_params = None

    if args.config:
        config.read(args.config)
        source = config.get('settings', 'source', fallback=args.source)
        input_file = config.get('settings', 'input', fallback=args.input)
        target = config.get('settings', 'target', fallback=args.target)
        query = config.get('settings', 'query', fallback=args.query)

        # Extract database connection parameters from config
        if source == "db":
            connection_params = {
                'dbname': config.get('database', 'dbname'),
                'user': config.get('database', 'user'),
                'password': config.get('database', 'password'),
                'host': config.get('database', 'host'),
                'port': config.get('database', 'port')
            }
    else:
        source = args.source
        input_file = args.input
        target = args.target
        query = args.query

    # Choose the appropriate input reader based on the source type
    if source == "file":
        input_reader = MemoryMappedFileInputReader(input_file)
    elif source == "db":
        input_reader = DatabaseInputReader(connection_params, query)
    else:
        raise ValueError("Source must be either 'file' or 'db'")

    # Initialize output writer, sanitizer, and statistics calculator
    output_writer = FileOutputWriter(target)
    sanitizer = ProcessTextSanitizer()
    statistics_calculator = AlphabetStatisticsCalculator()

    # Process the text
    processor = TextProcessor(input_reader, output_writer, sanitizer, statistics_calculator)
    processor.process()

if __name__ == "__main__":
    main()
