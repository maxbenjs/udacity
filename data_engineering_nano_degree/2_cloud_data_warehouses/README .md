# Project: Data Warehouse
---

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


### Schema Design
We've created a star schema optimized for queries on song play analysis, inlcuding the following tables:

#### Fact Table
##### songplays
    - records in event data associated with song plays i.e. records with page NextSong
        - songplay_id
        - start_time
        - user_id
        - level
        - song_id
        - artist_id
        - session_id
        - location
        - user_agent


#### Dimension Tables
##### users
- users in the app
    -  user_id
    - first_name
    - last_name
    - gender
    - level

##### songs
- songs in music database
    -  song_id
    - title
    - artist_id
    - year
    - duration

##### artists
- artists in music database
    -  artist_id
    - name
    - location
    - lattitude
    - longitude

##### time
    - timestamps records in songplays broken down into specific units
        - start_time
        - hour
        - day
        - week
        - month
        - year
        - weekday



### Scripts
The default list of files are dwh.cfg, create_tables.py, sql_queries.py, etl.py. Values were added to dwh.cfg, with minimal changes/additions to create_tables.py & etl.py. Specifically descriptions were added to the functions and arguments from the config file were fetched to conneect to the database.


Within sql_queries.py all all of the SQL statements were written and added to the variable placeholders.


There 2 new scripts (create_cluster.py & delete_cluster.py) which can be executed to create & delete the redshift clusters.

The testing_environment notebook was created to test functions before committing them to the placeholder python scripts. It was also used to run each of the scripts for the etl.


### Instructions
We can run python scripts within Notebooks using '%run <script_name.py' and run the following commands:


- %run create_cluster.py
- %run create_tables.py
- %run etl.py
- %run delete_cluster.py