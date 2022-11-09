staging_uk_crime_events = {
      'bucket_name': 'udac5capstoneproject'
    , 'table_name': 'staging_uk_crime_events'
    , 'dir_name': 'data/uk_crime_data'
    , 'file_name': 'UK_Police_Street_Crime_2018-10-01_to_2021_09_31.csv'
    , 'dwh_staging_table':'staging_uk_crime_events'
    , 'create_table_statement': """
            CREATE TABLE IF NOT EXISTS public.staging_uk_crime_events (
                  crime_id varchar(256)
                , event_month varchar(256)
                , reported_by varchar(256)
                , longitude decimal(16,8)
                , latitude decimal(16,8)
                , location varchar(256)
                , lsoa_code varchar(256)
                , lsoa_name varchar(256)
                , crime_type varchar(256)
                , last_outcome_category varchar(256)
        );
        """
    , 'datasource':'s3'
    , 'expected_primary_key':'false'
}


staging_uk_population = {
      'bucket_name': 'udac5capstoneproject'
    , 'table_name': 'staging_uk_population'
    , 'dir_name': 'data/uk_helper_data'
    , 'file_name': 'uk_regional_population_1997_2016.csv'
    , 'dwh_staging_table':'staging_uk_population'
    , 'create_table_statement': """
            CREATE TABLE IF NOT EXISTS public.staging_uk_population (
                  region varchar(256)
                , lau1_code varchar(256)
                , la_name varchar(256)
                , year INT
                , population INT
        );
        """
    , 'datasource':'s3'
    , 'expected_primary_key':'false'
}


staging_uk_gdi = {
      'bucket_name': 'udac5capstoneproject'
    , 'table_name': 'staging_uk_gdi'
    , 'dir_name': 'data/uk_helper_data'
    , 'file_name': 'uk_regional_gross_disposable_income_1997_2016.csv'
    , 'dwh_staging_table':'staging_uk_gdi'
    , 'create_table_statement': """
            CREATE TABLE IF NOT EXISTS public.staging_uk_gdi (
                  region varchar(256)
                , lau1_code varchar(256)
                , la_name varchar(256)
                , year INT
                , gross_disposable_income_gbp INT
        );
        """
    , 'datasource':'s3'
    , 'expected_primary_key':'false'
}



h_location_mapping = {
      'bucket_name': 'udac5capstoneproject'
    , 'table_name': 'h_location_mapping'
    , 'dir_name' : 'data/uk_postcode_conversion'
    , 'file_name': 'uk_postcode_mapping.csv'
    , 'dwh_staging_table':'h_location_mapping'
    , 'create_table_statement': """
            CREATE TABLE IF NOT EXISTS public.h_location_mapping (
                  postcode varchar(256)
                , lsoa_code varchar(256)
                , lau1_code varchar(256)
                , lsoa_name varchar(256)
                , primary key(postcode, lsoa_code, lau1_code, lsoa_name)
        );
        """
    , 'datasource':'s3'
    , 'expected_primary_key':'true'
}



d_location_staging = {
      'table_name': 'd_location_staging'
    , 'create_table_statement': """
            create table d_location_staging (
              lsoa_code varchar(256)
            , lsoa_name varchar(256)
            , longitude decimal(16,8)
            , latitude decimal(16,8)
            , location varchar(256)    
           );
        """
    , 'insert_target_columns': 'lsoa_code, lsoa_name, longitude, latitude, location'
    , 'insert_statement':"""
        select distinct
              lsoa_code
            , lsoa_name 
            , longitude
            , latitude 
            , location
        from staging_uk_crime_events
    """
    , 'datasource':'dwh'
    , 'expected_primary_key':'false'
}

d_location = {
      'table_name': 'd_location'
    , 'dwh_staging_table': 'd_location_staging'
    , 'dwh_target_table': 'd_location'
    , 'create_table_statement': """
            create table d_location (
              location_id INT identity(1, 1) primary key
            , lsoa_code varchar(256)
            , lsoa_name varchar(256)
            , longitude decimal(16,8)
            , latitude decimal(16,8)
            , location varchar(256)    
           );
        """
    , 'columns_to_update': """
              lsoa_code = d_location_staging.lsoa_code 
            , lsoa_name = d_location_staging.lsoa_name 
            , longitude = d_location_staging.longitude 
            , latitude = d_location_staging.latitude 
            , location = d_location_staging.location
    """
    , 'merge_key': """
            d_location.lsoa_code = d_location_staging.lsoa_code 
        and d_location.lsoa_name = d_location_staging.lsoa_name 
        and d_location.longitude = d_location_staging.longitude 
        and d_location.latitude = d_location_staging.latitude 
        and d_location.location = d_location_staging.location
    """
    , 'insert_target_columns': 'lsoa_code, lsoa_name, longitude, latitude, location'
    , 'insert_statement':"""
        select distinct
              lsoa_code
            , lsoa_name 
            , longitude
            , latitude 
            , location
        from d_location_staging
    """
    , 'datasource':'dwh'
    , 'expected_primary_key':'true'
}

d_investigation_outcome_staging = {
      'table_name': 'd_investigation_outcome_staging'
    , 'create_table_statement': """
            create table d_investigation_outcome_staging (
               investigation_outcome_category VARCHAR(256)        
        );
        """
    , 'insert_target_columns': 'investigation_outcome_category'
    , 'insert_statement':"""
        select distinct
              last_outcome_category as investigation_outcome_category
        from staging_uk_crime_events
    """
    , 'datasource':'dwh'
    , 'expected_primary_key':'false'
}

d_investigation_outcome = {
      'table_name': 'd_investigation_outcome'
    , 'dwh_staging_table': 'd_investigation_outcome_staging'
    , 'dwh_target_table': 'd_investigation_outcome'
    , 'create_table_statement': """
            create table d_investigation_outcome (
              investigation_outcome_id INT identity (1,1) primary key
            , investigation_outcome_category VARCHAR(256)       
        );
        """
    , 'columns_to_update': """
              investigation_outcome_category = d_investigation_outcome_staging.investigation_outcome_category 
    """
    , 'merge_key': """
            d_investigation_outcome.investigation_outcome_category = d_investigation_outcome_staging.investigation_outcome_category
    """
    , 'insert_target_columns': 'investigation_outcome_category'
    , 'insert_statement':"""
        select distinct
              investigation_outcome_category
        from d_investigation_outcome_staging
    """
    , 'datasource':'dwh'
    , 'expected_primary_key':'false'
}

f_uk_crime_events_staging = {
      'table_name': 'f_uk_crime_events_staging'
    , 'create_table_statement': """
           create table f_uk_crime_events_staging (
              crime_id varchar(256)
            , event_month varchar(256)
            , reported_by varchar(256)
            , location_id INT
            , investigation_outcome_id INT
           );
        """
    , 'insert_target_columns': 'crime_id, event_month, reported_by, location_id, investigation_outcome_id'
    , 'insert_statement':"""
        select
              fsuce.crime_id
            , fsuce.event_month
            , fsuce.reported_by
            , dl.location_id
            , dic.investigation_outcome_id
        from staging_uk_crime_events fsuce
        inner join d_location dl 
            on fsuce.lsoa_code = dl.lsoa_code
                and fsuce.lsoa_name = dl.lsoa_name
                    and fsuce.latitude = dl.latitude
                and fsuce.longitude = dl.longitude
            and fsuce.location = dl.location
        inner join d_investigation_outcome dic 
            on fsuce.last_outcome_category= dic.investigation_outcome_category
    """
    , 'datasource':'dwh'
    , 'expected_primary_key':'false'
}

f_uk_crime_events = {
      'table_name': 'f_uk_crime_events'
    , 'dwh_staging_table': 'f_uk_crime_events_staging'
    , 'dwh_target_table': 'f_uk_crime_events'
    , 'create_table_statement': """
            create table f_uk_crime_events (
              sk_crime_id INT identity(1, 1) primary key
            , crime_id varchar(256)
            , event_month varchar(256)
            , reported_by varchar(256) 
            , location_id INT
            , investigation_outcome_id INT
           );
        """
    , 'columns_to_update': """
                crime_id = f_uk_crime_events_staging.crime_id
              , event_month = f_uk_crime_events_staging.event_month
              , reported_by = f_uk_crime_events_staging.reported_by
              , location_id = f_uk_crime_events_staging.location_id
              , investigation_outcome_id = f_uk_crime_events_staging.investigation_outcome_id
"""
    , 'merge_key': """
                f_uk_crime_events.crime_id = f_uk_crime_events_staging.crime_id
              and f_uk_crime_events.event_month = f_uk_crime_events_staging.event_month
              and f_uk_crime_events.reported_by = f_uk_crime_events_staging.reported_by
              and f_uk_crime_events.location_id = f_uk_crime_events_staging.location_id
              and f_uk_crime_events.investigation_outcome_id = f_uk_crime_events_staging.investigation_outcome_id
    """
    , 'insert_target_columns': 'crime_id, event_month, reported_by, location_id, investigation_outcome_id'
    , 'insert_statement':"""
        select distinct
            crime_id
          , event_month
          , reported_by
          , location_id
          , investigation_outcome_id
        from f_uk_crime_events_staging
    """
    , 'datasource':'dwh'
    , 'expected_primary_key':'true'
}


f_uk_population = {
      'table_name': 'f_uk_population'
    , 'dwh_staging_table': 'staging_uk_population'
    , 'dwh_target_table': 'f_uk_population'
    , 'create_table_statement': """
            CREATE TABLE IF NOT EXISTS public.f_uk_population (
                  uk_population_id INT identity(1, 1) primary key
                , region varchar(256)
                , lau1_code varchar(256)
                , la_name varchar(256)
                , year INT
                , population INT
        );
        """
    , 'columns_to_update': """
              region = staging_uk_population.region 
            , lau1_code = staging_uk_population.lau1_code 
            , la_name = staging_uk_population.la_name 
            , year = staging_uk_population.year 
            , population = staging_uk_population.population
    """
    , 'merge_key': """
            f_uk_population.region = staging_uk_population.region 
        and f_uk_population.lau1_code = staging_uk_population.lau1_code 
        and f_uk_population.la_name = staging_uk_population.la_name
        and f_uk_population.year = staging_uk_population.year 
        and f_uk_population.population = staging_uk_population.population
    """
    , 'insert_target_columns': 'region, lau1_code, la_name, year, population'
    , 'insert_statement':"""
        select distinct
                region
              , lau1_code
              , la_name
              , year
              , population
        from staging_uk_population
    """
    , 'datasource':'dwh'
    , 'expected_primary_key':'true'
}



f_uk_gdi = {
      'table_name': 'f_uk_gdi'
    , 'dwh_staging_table': 'staging_uk_gdi'
    , 'dwh_target_table': 'f_uk_gdi'
    , 'create_table_statement': """
            CREATE TABLE IF NOT EXISTS public.f_uk_gdi (
                  uk_gdi_id INT identity(1, 1) primary key
                , region varchar(256)
                , lau1_code varchar(256)
                , la_name varchar(256)
                , year INT
                , gross_disposable_income_gbp INT
        );
        """
    , 'columns_to_update': """
              region = staging_uk_gdi.region 
            , lau1_code = staging_uk_gdi.lau1_code 
            , la_name = staging_uk_gdi.la_name 
            , year = staging_uk_gdi.year 
            , gross_disposable_income_gbp = staging_uk_gdi.gross_disposable_income_gbp
    """
    , 'merge_key': """
            f_uk_gdi.region = staging_uk_gdi.region 
        and f_uk_gdi.lau1_code = staging_uk_gdi.lau1_code 
        and f_uk_gdi.la_name = staging_uk_gdi.la_name
        and f_uk_gdi.year = staging_uk_gdi.year 
        and f_uk_gdi.gross_disposable_income_gbp = staging_uk_gdi.gross_disposable_income_gbp
        
    """
    , 'insert_target_columns': 'region, lau1_code, la_name, year, gross_disposable_income_gbp'
    , 'insert_statement':"""
        select distinct
                region
              , lau1_code
              , la_name
              , year
              , gross_disposable_income_gbp
        from staging_uk_gdi
    """
    , 'datasource':'dwh'
    , 'expected_primary_key':'true'
}



staging_tables = [
      staging_uk_crime_events
    , staging_uk_population
    , staging_uk_gdi
    , d_location_staging
    , d_investigation_outcome_staging
]

helper_tables = [h_location_mapping]


dim_tables = [
      d_location
    , d_investigation_outcome
]

staging_fact_tables = [f_uk_crime_events_staging]

fact_tables = [f_uk_crime_events, f_uk_population, f_uk_gdi]