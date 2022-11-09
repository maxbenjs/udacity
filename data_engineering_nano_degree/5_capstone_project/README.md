# Data Engineering Capstone Project: UK Crime Data
---
### 1. Overview
---
The capstone project for the Udacity DataEngineering Nanodegree, aims to allow students to apply components learned throughout the program into a final project.

#### Datasets
3 different datasets were used:


##### UK Crime Data
- UK Street Crime (2018-2021, +18m records, 820 MB Zipped, 4.2GB Unzipped, CSV)
- Data contains annonimzyed, street level crime data
- Link: https://www.kaggle.com/datasets/tantable/all-uk-police-street-crime-102018-to-092021

###### UK Gross Disposable Income & Population Data
- Annualized aggregate gross disposable income and population data on a UK local authority level from 1997 - 2016
- https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland

##### UK Postcode mapping data
- Postcode to Output Area Hierarchy with Classifications data (+2.6m records, 120 MB Zipped, +400 MB Unzipped, CSV)
- Data is need to mapping Given the different geo locational data used in the Crime Data & Income/GDI Data
- https://geoportal.statistics.gov.uk/datasets/c522a886a4b2497f82e797b124d9ba65


#### Goal
This final project will build a date pipeline for UK Crime data. Whilst the outcome is a fully functioning data pipeline, this project was built on the assumption of building a fully functional ETL & Cloud Datawarehouse to enable an analytics team for ad-hoc analyses and integrate the data into a live BI Reporting solution.


#### Technologies
The formatted data files are stored on Amazon S3, which is is an object storage service that offers industry-leading scalability, data availability, security, and performance. S3 has low cost and lots of flexibility.

The datawahouse is built on Amazon Redshift, which is a cloud-based petabyte-scale data warehouse service offered as one of Amazon's ecosystem of data solutions.



### 2. Explore Data Quality
---
First we explore each of the datasets to identify quality issues including missing values and duplicates.

The UK Crime Data is largely very high quality. In 25% of the cases the crime id is missing. This data was removed. The context column has also been removed since the field is always null. The 'Falls Within' field was also removed since it always equals 'Reported By' and we only need one of the fields. 

The Crime Helper Data (Population & GDI) comes from an Excel sheet and requires a greater amount of transformation. We load the selected sheets into separate dataframes, before ensuring the header responds to the target column data. Finally we unpivot the yearly Population & GDI data to ensure it exists on a row-by-row level. Finally rows with null removed are removed, as well as any innaccurate trailing rows of data.

The postcode mapping dataset requires some transformation, including removing rows with empty values, renaming columns consistant with the fields in our other datasets, before removing any columns which aren't required.


Some of the biggest limitations within the data are the inconsistent time frames betweeen the Crime Data (2018 - 2021) and the helper population & gdi data (1997 - 2016). To factor the population/gdi into our crime analysis we could either:
- Find updated population/gdi data if possible
- Use the latest population/gdi values within the dataset
- Forecast current population/gdi values based on the prior 19 years (Arguably over-engineered)


### 3.1 Data Model
---

The purpose of this data warehouse is to provide a single source of truth, store different kinds of data and restructure the data to optimize it for data analysis and a BI Reporting tool such as Looker or Tableau.

For this reason we use a star schema to split the data into fact and dimension tables, where the dimension tables frequently include fields on which datasets & visualisations will be filtered.

The data model has been overengineered for the scope of this project where the ETL would only have to be executed once. The data model has therefore been designed under the assumption, that the data sources would be updated frequently, requiring scheduled and frequent loading of new data. We assume that the data would not only have to be inserted, but merged instead (also known as an upsert). Considering this, in addition to the fact that Redshift doesn't have upsert statements, we use staging tables for all of the dimension & fact tables in which we would want to merge (upsert) data on a frequent basis.


### 3.2 Data Dictionary
---
#### Staging Tables:

###### staging_uk_crime_events
- crime_id varchar unique crime id provided in original dataset
- event_month varchar month in which the crime occurs
- reported_by varchar polica authority which reported the crime
- longitude decimal longitude of the crime location
- latitude decimal latitude of the crime location
- location varchar crime location name
- lsoa_code varchar crime location lsoa_code
- lsoa_name varchar crime location lsoa_name
- crime_type varchar type of crime
- last_outcome_category varchar the last outcome of the crime

###### staging_uk_population
- region varchar  region name
- lau1_code varchar local authority lau1 code
- la_name varchar local authority name
- year INT year in which the population is counted
- population INT population number

###### staging_uk_gdi
- region varchar region name
- lau1_code varchar local authority lau1 code
- la_name varchar local authority name
- year INT year in which the gross disposable income is counted
- gross_disposable_income_gbp INT gross disposable income in british pounds


###### d_location_staging
- postcode varchar british location postcode
- lsoa_code varchar location lsoa code
- lau1_code varchar location lau1 code
- lsoa_name varchar location lsoa name 


###### d_investigation_outcome_staging
- investigation_outcome_category varchar  the last outcome of the crime


###### f_uk_crime_events_staging
- sk_crime_id INT surrogate key for the crime id
- crime_id varchar unique crime id provided in original dataset
- event_month varchar month in which the crime occurs
- reported_by varchar polica authority which reported the crime
- location_id INT id of the crime location
- investigation_outcome_id INT id for the investigation outcome

#### Dimension Tables:

###### d_location
- location_id INT of the crime location
- lau1_code varchar local authority lau1 code
- la_name varchar local authority name
- latitude decimal latitude of the crime location
- location varchar crime location name
- location varchar crime location name

###### d_investigation_outcome
- investigation_outcome_id INT  id for the investigation outcome
- investigation_outcome_category name of the investigation outcome


#### Fact Tables:
###### f_uk_crime_events
- sk_crime_id INT surrogate key for the crime id
- crime_id varchar unique crime id provided in original dataset
- event_month varchar month in which the crime occurs
- reported_by varchar polica authority which reported the crime
- location_id INT id of the crime location
- investigation_outcome_id INT id for the investigation outcome

###### f_uk_population
- uk_population_id INT surrogate key for the population id
- region varchar  region name
- lau1_code varchar local authority lau1 code
- la_name varchar local authority name
- year INT year in which the population is counted
- population INT population number

###### f_uk_gdi
- uk_gdi_id INT identity surrogate key for the uk gdi id
- region varchar region name
- lau1_code varchar local authority lau1 code
- la_name varchar local authority name
- year INT year in which the gross disposable income is counted
- gross_disposable_income_gbp INT gross disposable income in british pounds

#### Helper Tables:
###### h_location_mapping
- postcode varchar british location postcode
- lsoa_code varchar location lsoa code
- lau1_code varchar location lau1 code
- lsoa_name varchar location lsoa name 


### 4. Project Structure & ETL Orchestration
---
#### ETL Steps:
1. Extract data from zip files
2. Format data/csvs
3. Push data to S3
4. Check if DWH Tables exists >> Create table if missing
5. Load Data (Staging Tables)
6. Load Data (Dim Tables)
7. Load Data (Fact Tables)
8. Delete Staging Data
9. Delete S3 File

###### Data Directory
- Contains 3 sub directories containing raw data from the 3 data sources
- 3 New directories are created, corresponding to each of the 3 original data sources which contain the formatted data, prior to the s3 upload

###### helpers/helpers_python Directory
- database_class.py: File containing database class which allows us the connect to the data warehouse and carry out the respective tasks required (e.g. create tables, insert statements, merge statements etc.)
- aws_s3_class.py: File containing aws class which allows us to connect to s3, upload & delete files 
- python_helpers.py: File containing multiple python helper functions which are used within the ETL to extract and transform the various data sourcs

###### helpers/helpers_sql Directory
- table_params.py: Python file which contains a dictionary for every table, with all of the relevant information requiried for that table through out the ETL, including specific create table statements, 
- sql_statements.py: Python file which contains the various unformatted sql statements required to load, insert, mergee and delete within the dwh. Sql statements are dynamically formatted within ETL.


#### Notebooks, Python Files & Config
- Capstone Project Notebook.ipynb: Notebook which break downs the invidual steps within the entire ETL.
- ETL Notebook.ipynb: Notebook which builds & tests the final ETL script
- ETL.py: Python script for the ETL
- dl.cfg: Config file which contains access keys and credentials for AWS, S3 & DWH


### 5. Further Notes
---
#### Scenario Planning: Changes under the following scenarios:

##### 1. The data was increased by 100x
- If we assume that raw crime data was 100x larger, but new data would be provided on a daily level (instead of 3 historical years), our daily crime data would - assuming we compressed it - approximately 380 MB uncompresseed & 70 MB compressed (4.2GB/800MB ÷(3 X 365) X 100).

- We would still be able use a Redshift data warehouse to process this data, but might want to improve the performance by upgrading our cluster / nodes. We could also consider moving to a Big Data infrastructure, running Spark on an EMR cluster.


- If we assume that the data was 100x larger, and would want to load entire 3 years of data the data would be 420GB/70 GB uncompressed/compressed. In this insttance we would definitely want to create a Big Data Pipepiline, running Spark on an EMR cluster.


- In a production system we would be running this pipeline using Airflow running on a clustere of machines. We would increase the worker instance count to improve the execution time, following the increase in data size.

- We would still use S3 Storage which is allows the storing of big data (max. file size 5 TB). We would consider partioning the raw data.


##### 2. The data populates a dashboard that must be updated on a daily basis by 7am every day
- In a production system we would run this pipeline using Airflow and schedule the pipeline to run so that the updated data is avaiable by 7am.


##### 3. The database needed to be accessed by 100+ people
- We would have to consider the limitations of the Redshift server, including the rate limits and consider upgrading the server. We could also move to Redshift serverless, which would eliminate some of the server related limitations.

- Within a serverless environment we would consider the limits for StartQueryExecution API calls.


### 6. Running the ETL
---
Within ETL Notebook, execute '%run ETL.py' within a cell.
- Improving running times for testing:
    - Biggest bottlenecks are the 2 larger datasetes (UK Crime Data & Postcode mapping data)
    - Reduce the size of these 2 csv files, by limiting the rows of the df/csv
    - helpers/helpers_python/python_functions.py
    - For extract_and_clean_crime_dataset() & extract_and_clean_postcode_helper_dataset(), insert nrows into read_csv
    - Given the patchy internet where this ETL was built and tested, running times have varied from 16m (100% of dataset) to 15m (5% of dataset)
        
        