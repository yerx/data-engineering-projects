# Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Description
 I used Postgres as my database and implemented the star schema to model the data and built an ETL pipeline using Python to analyze the song data.

## Schema for Song Play Analysis
Implemented a star schema for song play analysis.

**Fact Table**
1. **songplays** - records in log data associated with song plays
* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
2. **users** - users in the app
* user_id, first_name, last_name, gender, level

3. **songs** - songs in music database
* song_id, title, artist_id, year, duration

4. **artists** - artists in music database
* artist_id, name, location, latitude, longitude

5. **time** - timestamps of records in songplays broken down into specific units
* start_time, hour, day, week, month, year, weekday

## ETL Pipeline
1. Create the Sparkify database and used the star schema to create the tables
2. Extract data from the songs and log data files
3. Transform the data 
4. Load the data into the fact and dimensional tables

## Project Files
* **create_tables.py** contains scripts to drop and create create the database and tables
* **sql_queries.py** contains the data tables and sql queries
* **etl.ipynb** similar to the etl.py file, created this file first to develop the script to load, read, and process data using Jupyter Notebook to display the results
* **etl.py** contains script to load the data into the database and read and process the data
* **test.ipynb** used to verify that the database and tables have loaded properly and are outputting the desired results
* **data** folder contains raw data for analysis
* README.md provides a description of the project

## How to Run the Project
1. In the terminal, run `python create_tables.py`
3. In the terminal, run `python etl.py`
4. Run test.ipynb in Jupyter Notebook to verify that the tables were created and that the queries are returning the correct results.


