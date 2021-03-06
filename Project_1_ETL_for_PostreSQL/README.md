# Project #1: Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
* songplay_id, 
* start_time, 
* user_id, 
* level, 
* song_id, 
* artist_id, 
* session_id, 
* location, 
* user_agent
### Dimension Tables
**users** - users in the app
* user_id, 
* first_name, 
* last_name, 
* gender, 
* level
**songs** - songs in music database
* song_id, 
* title, 
* artist_id, 
* year, 
* duration
**artists** - artists in music database
* artist_id, 
* name, 
* location, 
* latitude, 
* longitude
**time** - timestamps of records in songplays broken down into specific units
* start_time, 
* hour, 
* day, 
* week, 
* month, 
* year, 
* weekday

<img width="785" alt="Screenshot_2022-02-27_at_10 59 17_AM" src="https://user-images.githubusercontent.com/83006335/176858895-b839549c-5f87-4fef-8e9c-aec86b67bd41.png">

## ETL Pipeline
The file etl.py contatins ETL pipeline and perforoms the following steps:

* Connect to the database.
* Process song files.
* Insert song data into **songs** table.
* Insert artist data into **artists** table.
* Process log_files.
* Insert date data into **time** table.
* Insert user info into **users**  table.
* Insert songpplay records into **songplays** table. 
* Disconnect from the database.

## Run the scripts
Run the first **create_tables.py** to create empty tables
> python3 create_tables.py

Run the second script **etl.py** to ETL data to created tebles
> python3 etl.py

Run the Jupyter notebook **test.ipynb** if needed to test and check filled tables
