# Project: Data Modeling with Cassandra
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

## Task Description 
- Create an Apache Cassandra database which can create queries on song play data to answer the questions. 
- Be able to test the database by running Predefined queries.
- Create ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into apache Cassandra tables.

## Datasets
event_data. Is the dataset for this project. It contains a directory of CSV files partitioned by date.

Here are examples of filepaths to two files in the dataset.
- event_data/2018-11-08-events.csv
- event_data/2018-11-09-events.csv

## Project steps
### Modeling NoSQL database / Apache Cassandra database.
- Design tables to answer the queries outlined in the project template.
- Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
- Develop CREATE statement for each of the tables to address each Query question
- Load the data with INSERT statement for each of the tables
- RRecommendation(Include IF NOT EXISTS clauses in  CREATE statements to create tables only if the tables do not already exist,include DROP TABLE statement for each table, this helps to reset database and test ETL pipeline
- Test by running the proper select statements with the correct WHERE clause. 

## Build ETL Pipeline
- Iterate through each event file in event_data to Extract, process and create a new CSV file in Python (Extract and Transform)
- Using Apache Cassandra CREATE and INSERT statements to load processed records into relevant tables in your data model (load)
- Test by running SELECT statements after running the queries
