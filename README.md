World Bank Data Processing

Overview

This project aims to automate the process of compiling and transforming data on the top 10 largest banks in the world, ranked by market capitalization. The data is extracted from a web source, transformed into multiple currencies based on exchange rates, and then saved in both CSV format and an SQL database. The system is designed to be run quarterly to generate up-to-date reports.

Project Structure

banks_project.py: The main script containing functions for data extraction, transformation, loading, and querying.
code_log.txt: Log file that tracks the progress and status of the script execution.
Largest_banks_data.csv: Output CSV file containing the processed bank data.
Banks.db: SQLite database file storing the bank data in a table.
Functionality

Data Extraction: Fetches tabular data on the largest banks from a specified URL and converts it into a DataFrame.
Data Transformation: Adds columns for market capitalization in GBP, EUR, and INR based on exchange rates provided in a CSV file. Values are rounded to two decimal places.
Data Loading:
CSV: Saves the transformed data to a CSV file.
SQL: Saves the transformed data to an SQLite database table.
Database Queries: Runs queries to extract market capitalization data in various currencies for specific regional offices.
Logging: Tracks and logs the progress of each stage of the script execution.
