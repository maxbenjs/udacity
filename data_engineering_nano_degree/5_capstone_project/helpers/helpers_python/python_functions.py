import os
import zipfile
import pandas as pd


cwd = os.getcwd()


def zip_downloader(input_dir, output_dir):
    """
    Function to extract zip files from input directory to target output directory
    """
        
    output_path = os.path.join(cwd, output_dir)
    input_path = os.path.join(cwd, input_dir)
    
    try:
        for file in os.listdir(input_path):
            if not file.endswith('.zip'):
                pass
            
            else:
                zip_file_path = os.path.join(input_path, file)
                zip_file_path = zipfile.ZipFile(zip_file_path, 'r') 
                
                if not os.path.exists(os.path.join(cwd, output_dir)):
                    os.mkdir(os.path.join(cwd, output_dir))
                    
                zip_file_path.extractall(output_path)
            

    except Exception as e:
        print(e)
        



def extract_and_clean_crime_dataset(input_dir, input_file_name, output_dir, output_file_name):
    
    input_dir = input_dir
    input_file_name = input_file_name
    filepath = os.path.join(cwd, input_dir, input_file_name)
    df = pd.read_csv(filepath_or_buffer=filepath, nrows=1000000)
    print(df.head(3))
    
    
    try:
        ### Remove columns
        df = df.drop(columns={'Unnamed: 0', 'Context', 'Falls within'})


        ### Rename columns
        df = df.rename(columns={
              'Crime ID': 'crime_id'
            , 'Month': 'event_month'
            , 'Reported by': 'reported_by'
            , 'Longitude': 'longitude'
            , 'Latitude': 'latitude'
            , 'Location': 'location'
            , 'LSOA code': 'lsoa_code'
            , 'LSOA name': 'lsoa_name'
            , 'Crime type': 'crime_type'
            , 'Last outcome category': 'last_outcome_category'
        })
        
        df['last_outcome_category'] = df['last_outcome_category'].astype(str)
        df['last_outcome_category'] = df['last_outcome_category'].apply(lambda x: x.replace(';',','))
        
        output_dir = output_dir
        output_file_name = output_file_name
            
        if not os.path.exists(os.path.join(cwd, output_dir)):
            os.mkdir(os.path.join(cwd, output_dir))
        
        output_filepath = os.path.join(cwd, output_dir, output_file_name)

        ### Save to csv
        df.to_csv(output_filepath, index=False, compression='gzip', sep = ';')
        
        print(f'Extracted and transformed crime dataset: {output_file_name}')
        
        return df
    
    except Exception as e:
        print(e)
        
        

def extract_and_clean_crime_helper_dataset_gdi(input_dir, input_file_name, output_dir, output_file_name):
    
    input_dir = input_dir
    input_file_name = input_file_name 
    input_filepath = os.path.join(cwd, input_dir, input_file_name)
    
    try:
        f = pd.ExcelFile(input_filepath)

        ##Target excel sheet
        target_sheets = ['Gross disposable income']

        ## Load target excel sheet into df
        df = f.parse(target_sheets[0])

        ## Ensure row 2 data (target columns) is df columns
        df.columns = df.iloc[1]
        df = df[2:]

        #Unpivot gdi column data to rows
        df = df.melt(id_vars=['Region','LAU1 code','LA name'])
        df = df.rename(columns={1:'year', 'value':'gdi_1000s','Region':'region','LAU1 code':'lau1_code','LA name':'la_name'})

        ### Ensure accurate data types for integers & create gross_disposable_income column
        df['gdi_1000s'] = df['gdi_1000s'].fillna(0) 
        df['gross_disposable_income_gbp'] = df['gdi_1000s']*1000 
        df['year'] = df['year'].astype(int)
        df['gross_disposable_income_gbp'] = df['gross_disposable_income_gbp'].astype(int)

        ## Remove rows with null values
        df = df.dropna()
        
        ## Remove 'gdi_1000s' column
        df = df.drop(columns=['gdi_1000s'])

        ### Remove final 2 rows
        df.drop(df.tail(2).index,inplace=True)

        ### Save df to csv
        output_dir = output_dir
        output_file_name = output_file_name

        if not os.path.join(cwd, output_dir):
            os.mkdir(os.path.join(cwd, output_dir))

        filepath = os.path.join(cwd, output_dir, output_file_name)
        df.to_csv(filepath, index=False, compression='gzip', sep = ';')
        
        print(f'Extracted and transformed gdi dataset: {output_file_name}')
    
    except Exception as e:
        print(e)

    
    
def extract_and_clean_crime_helper_dataset_population(input_dir, input_file_name, output_dir, output_file_name):
    
    try:
        input_dir = input_dir
        input_file_name = input_file_name 
        input_filepath = os.path.join(cwd, input_dir, input_file_name)

        f = pd.ExcelFile(input_filepath)

        ##Target excel sheet
        target_sheets = ['Population']

        ## Load target excel sheet into df
        df = f.parse(target_sheets[0])

        ## Ensure row 2 data (target columns) is df columns
        df.columns = df.iloc[1]
        df = df[2:]

        #Unpivot population column data to rows
        df = df.melt(id_vars=['Region','LAU1 code','LA name'])
        df = df.rename(columns={1:'year', 'value':'population','Region':'region','LAU1 code':'lau1_code','LA name':'la_name'})


        ### Ensure accurate data types for integers 
        df['year'] = df['year'].astype(int)
        df['population'] = df['population'].astype(int)

        ## Remove rows with null values
        df = df.dropna()

        ### Save df to csv
        output_dir = output_dir
        output_file_name = output_file_name

        if not os.path.exists(os.path.join(cwd, output_dir)):
            os.mkdir(os.path.join(cwd, output_dir))

        filepath = os.path.join(cwd, output_dir, output_file_name)
        df.to_csv(filepath, index=False, compression='gzip', sep = ';')

        print(f'Extracted and transformed population dataset: {output_file_name}')
    
    except Exception as e:
        print(e)
    
    
    

def extract_and_clean_postcode_helper_dataset(input_dir, input_file_name, output_dir, output_file_name):
    
    input_dir = input_dir
    input_file_name = input_file_name 
    input_filepath = os.path.join(cwd, input_dir, input_file_name)
    
    try:
        ## Load csv to df
        df = pd.read_csv(input_filepath, encoding='latin-1', nrows=1000000)

        ## Format df
        df = df.rename(columns={'pcd7':'postcode', 'lsoa11cd':'lsoa_code','ladcd':'lau1_code','lsoa11nm':'lsoa_name'})
        df = df[['postcode','lsoa_code','lau1_code','lsoa_name']]

        ## Remove rows with null values
        df = df.dropna()

        if not os.path.exists(os.path.join(cwd, output_dir)):
            os.mkdir(os.path.join(cwd, output_dir))

        ### Save df to csv
        filepath = os.path.join(cwd, output_dir, output_file_name)
        df.to_csv(filepath, index=False, compression='gzip', sep = ';')
        
        print(f'Extracted and transformed postcode helper dataset: {input_file_name}')
    
    except Exception as e:
        print(e)
    
        
    
