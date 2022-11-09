import psycopg2
import pandas as pd




class Database(object):
    
    
    def __init__(self, host, dbname, port, user, password):
        self.host = host
        self.dbname = dbname
        self.port = port
        self.user = user
        self.password = password
        self.conn = None
    
    
    
    def connect(self):
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    dbname=self.dbname
                )
            except psycopg2.DatabaseError as e:
                print(e)
    
    
    
    
    def query_to_df(self, sql):
        if self.conn is None:
            self.connect()
            
        df = pd.read_sql_query(sql, self.conn)
        return df
    
    
    
    
    def get_database_tables(self):
        if self.conn is None:
            self.connect()
            
        sql = "select distinct tablename from pg_table_def where schemaname = 'public'"        
        df = pd.read_sql(sql, self.conn)
        existing_database_tables = df['tablename'].to_list()
        return existing_database_tables


    
    
    def get_missing_database_tables(self, all_table_params):
        missing_database_tables = []
        existing_database_tables = self.get_database_tables()
        all_table_params = all_table_params
        target_database_tables = []
        

        for table_param in all_table_params:
            table_name = table_param['table_name']
            target_database_tables.append(table_name)
            
        
        for target_table in target_database_tables:
    
            if target_table not in existing_database_tables:
                missing_database_tables.append(target_table)
                
            else:
                pass
        
        print(f"Tables missing: {missing_database_tables}")
        
        return missing_database_tables
    
    
    
    
    def create_missing_database_tables(self, all_table_params):
        
        missing_database_tables = self.get_missing_database_tables(all_table_params=all_table_params)
        
        if len(missing_database_tables) == 0:
            print(f"All tables have already been created")
            pass

        else:    
            for table in missing_database_tables:

                for table_param in all_table_params:
                    if table == table_param['table_name']:
                        create_table_statement = table_param['create_table_statement']
                        
                        conn = self.conn
                        cur = conn.cursor()
                        cur.execute(create_table_statement)
                        conn.commit()
                        print(f"Created table: {table}")
                                                
                      


    def load_staging_tables_s3_based(self, tables, config, sql_copy_statements):
        
        
        for table in tables:
            
            if table['datasource'] != 's3':
                pass

            else:

                dwh_staging_table = table['dwh_staging_table']
                bucket_name = table['bucket_name']
                file_name = table['file_name']
                role_arn = config['IAMROLE']['user_arn']

                sql = sql_copy_statements.format(dwh_staging_table=dwh_staging_table
                                               , bucket_name=bucket_name
                                               , file_name=file_name
                                               , role_arn = role_arn)
                

                conn = self.conn
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()
                print(f"Data loaded for staging table (S3 Based): {dwh_staging_table}")
                
                


    def load_staging_tables_dwh_based(self, tables, sql_insert_statement):
        
        for table in tables:

            if table['datasource'] != 'dwh':
                pass

            else:

                table_name = table['table_name'] 
                insert_target_columns = table['insert_target_columns']
                insert_statement = table['insert_statement']

                sql = sql_insert_statement.format(dwh_target_table=table_name
                                                                   , insert_target_columns = insert_target_columns
                                                                   , insert_statement = insert_statement)
                

                conn = self.conn
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()
                print(f"Data loaded for staging table (DWH Based): {table_name}")
                
                
    
    def load_dim_tables(self, tables, sql_upsert_statement):

            for table in tables:
                
                dwh_target_table = table['table_name'] 
                columns_to_update = table['columns_to_update']
                dwh_staging_table = table['dwh_staging_table']
                merge_key = table['merge_key']
                insert_target_columns = table['insert_target_columns']
                insert_statement = table['insert_statement']
                
                sql = sql_upsert_statement.format(dwh_target_table = dwh_target_table
                                                                    , columns_to_update = columns_to_update
                                                                    , dwh_staging_table = dwh_staging_table
                                                                    , merge_key = merge_key
                                                                    , insert_target_columns = insert_target_columns
                                                                    , insert_statement = insert_statement)

                conn = self.conn
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()
                print(f"Data loaded for dim table (DWH Based): {dwh_target_table}")
                

               
    def load_fact_tables(self, tables, sql_upsert_statement):

            for table in tables:
                
                dwh_target_table = table['table_name'] 
                columns_to_update = table['columns_to_update']
                dwh_staging_table = table['dwh_staging_table']
                merge_key = table['merge_key']
                insert_target_columns = table['insert_target_columns']
                insert_statement = table['insert_statement']
                
                sql = sql_upsert_statement.format(dwh_target_table = dwh_target_table
                                                                    , columns_to_update = columns_to_update
                                                                    , dwh_staging_table = dwh_staging_table
                                                                    , merge_key = merge_key
                                                                    , insert_target_columns = insert_target_columns
                                                                    , insert_statement = insert_statement)

                conn = self.conn
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()
                print(f"Data loaded for fact table (DWH Based): {dwh_target_table}")
                

             

    def delete_staging_data(self, tables, sql_delete_statement):
        
        for table in tables:
            table_name = table['table_name']
            
            sql = sql_delete_statement.format(table_name = table_name)
            conn = self.conn
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            print(f"Data deleted from staging table: {table_name}")
            



    def qa_dim_fact_tables_contain_values(self, tables):
        
        conn = self.conn
        conn.autocommit = True
        
        for table in tables:
    
            table_name = table['table_name']
            sql = f'select count(*) from {table_name}'   
            df = self.query_to_df(sql)
            row_count = df['count'].values[0]

            if row_count > 0:
                print(f'QA Value Counts Success. Table ({table_name}): {row_count} rows inserted')

            else:
                print(f'QA Value Counts Failure. Table ({table_name}): {row_count} rows inserted')

  

    
    def qa_staging_tables_empty(self, tables):
        
        conn = self.conn
        conn.autocommit = True

        for table in tables:

            table_name = table['table_name']
            sql = f'select count(*) from {table_name}'
            df = self.query_to_df(sql)
            row_count = df['count'].values[0].astype(int)

            if row_count == 0:
                print(f'QA Empty Staging Tables Success. Table ({table_name}): {row_count} rows')

            else:
                print(f'QA Empty Staging Tables Failure. Table ({table_name}): {row_count} rows')

                    
                     