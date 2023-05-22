# DWH Project
> Build an simple ETL with Redshift in Dataware House

## Table of Contents
* [Create Tables](#create-tables)
* [ETL](#etl)
* [Sample Queries](#sample-queries)


## Create Tables
Create staging tables and analyzing tables in Redshift, drop all tables if they are existed
$ python create_tables.py 

## ETL
Extract json data from S3 into staging tables. Afterwards, continue to extract data from staging tables, then transform data and load them into analyzing tables
$ python etl.py

## Sample Queries
Some sample queries to analyze number of user
$ python analyze.py

### query_session_with_num_of_user
![screenshot 1](./img/query_session_with_num_of_user.png)


### query_on_song_artist_location
![screenshot 2](./img/query_on_song_artist_location.png)


### query_on_song_artist_latitude
![screenshot 3](./img/query_on_song_artist_latitude.png)


### query_on_song_artist_location_with_female
![screenshot 4](./img/query_on_song_artist_location_with_female.png)


### query_on_song_artist_location_with_female_paid
![screenshot 5](./img/query_on_song_artist_location_with_female_paid.png)