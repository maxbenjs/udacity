# sparkify-datalake-spark-etl
--- 
### Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.





## Datalake Schema
---

### Fact Table
- songplays (songplay records in log data): songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- users (app users): user_id, first_name, last_name, gender, level
- songs (songs in database): song_id, title, artist_id, year, duration
- artists (artists in database): artist_id, name, location, lattitude, longitude
- time (timestamps of songplay records, broken down into specific units): start_time, hour, day, week, month, year, weekday


## Files in Repository 
- README (Description of overall project)
- etl.py (Python Script for datalake-spark-etl)
- dl.cfg (Config file containing key value pairs of AWS Credentials & S3 Buckets)
- testing_datalake_tables.ipynb (Notebook which tests the datalake tables)


## Running on AWS
---
				
#### Create EMR Cluster

- EMR Cluster can be create within AWS or in the Terminal using AWS CLI:

				aws emr create-cluster \
				--name udacity-spark-cluster-cli \
				--use-default-roles \
				--release-label emr-5.28.0 \
				--instance-count 8 \
				--applications Name=Spark Name=Zeppelin \
				--ec2-attributes KeyName=udacity-spark-cluster,SubnetId=<subnet id> \
				--instance-type m5.2xlarge \
				--configurations file://spark_env.json \
				--profile <aws profile>


- Enable access permisions of certificate (pem file)

				chmod 400 <path of pem certificate>                                                    

- Copy etl.py and dl.cfg onto Spark Cluster

				scp -i <path of pem certificate> <path of etl script> <spark cluster Master public DNS>

				scp -i <path of pem certificate> <path of config file> <spark cluster Master public DNS>


- Login to Spark Cluster using SSH

				ssh -i <path of pem certificate> <spark cluster Master public DNS>



- Execute Script

				spark-submit etl.py



				



