from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def import_user_data():
    import pandas as pd
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    from unidecode import unidecode
    import difflib
    import pycountry
    import re
    # import logging

    # Set up logging
    # logging.basicConfig(level=logging.INFO)
    # logger = logging.getLogger(__name__)
    # logger.info("Starting import_user_data function")
    
    data_prefix_path = '/opt/airflow/data'
    cols_dict = {
        'user_id': 'Int64'
        , 'sex': 'string'
        , 'plan': 'string'
        , 'age': 'float'
        , 'age_bin': 'string'
        , 'ethnicity': 'string'
        , 'department': 'string'
        , 'nationality': 'string'
        , 'country_of_work': 'string'
    }
    
    # cast_cols = {col: 'string' for col in cols_dict.keys()}
    cols = [col for col in cols_dict.keys()]
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    def read_data():
        try: 
            df = pd.read_csv(f'{data_prefix_path}/users_data.csv', header=0, encoding='utf-8')
            df = df.replace(np.nan, '', regex=True)
            
            cast_cols = {col: 'string' for col in df.columns}
            
            df = df.astype(cast_cols)
            return df, True 
        
        except Exception as e:
            print(f"Error reading data: {e}")
            return None, False
    
    def clean_data(df):
        def clean_id(value):
            try:
                if pd.isna(value) or (str(value).strip() == ''):
                    return None
                
                if float(value) == int(float(value)):
                    return int(float(value))
                return -1
            
            except:
                return -1
 
        def clean_age(value):
            try:
                if pd.isna(value) or str(value).strip() == '':
                    return None
                return int(float(value))
            
            except:
                return -1
        
        def clean_sex(value):
            valid_sexes = ['Male', 'Female']
            if pd.isnull(value) or (str(value).strip() == ''):
                return None
            value = str(value).strip().title()
            
            match = difflib.get_close_matches(value, valid_sexes, n=1, cutoff=0.5)
            return match[0].lower().title() if match else "Unknown"
        
        def clean_age_bin(value):
            if pd.isnull(value) or (str(value).strip() == ''):
                return None
            value = value.split('~', 1)
            value[0] = value[0].strip()
            value[1] = value[1].strip()
            
            age = value[0]
            
            if (age == '') and (value[1] == ''):
                return None
            elif (age == '') and (value[1] != ''):
                s = value[1].strip()
                valid_age_bin = ['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '60+']
                
                match = difflib.get_close_matches(s, valid_age_bin, n=1, cutoff=0.5)
                return match[0].lower() if match else None
            
            
            age = int(float(age))
            if (age >=0) and (age <= 10):
                return '0-10'
            elif age >= 60:
                return '60+'
            elif age > 10:
                return f'{str(age)[0]}1-{int(int(age)/10 + 1)}0'
        
        def clean_nationality(value):
            valid_nationalities = [country.name for country in pycountry.countries]
            valid_nationalities += ['USA', 'UK', 'South Korea', 'North Korea', 'Vietnam', 'Russia', 'Hong Kong']
            
            if pd.isnull(value) or (str(value).strip() == ''):
                return None
            
            value = re.sub(r'[^a-zA-Z\- ]', '', str(value))
            value = value.strip().title()
            match = difflib.get_close_matches(value, valid_nationalities, n=1, cutoff=0.6)
            return match[0] if match else "Unknown"
        
        def clean_ethnicity(value):
            valid_ethnicity = ['Chinese', 'Malay', 'Indian', 'Others', ]
            if pd.isnull(value) or (str(value).strip() == ''):
                return None
            
            value = str(value).strip().title()
            
            match = difflib.get_close_matches(value, valid_ethnicity, n=1, cutoff=0.5)
            return match[0] if match else "Unknown"
        
        def clean_department(value):
            valid_department = ['business', 'hr', 'product']
            if pd.isnull(value) or (str(value).strip() == ''):
                return None
            
            value = str(value).strip().lower()
            
            match = difflib.get_close_matches(value, valid_department, n=1, cutoff=0.5)
            
            if match and (len(match) > 0):
                department = match[0]
                if department in ['business', 'product']:
                    return department.title()
                elif department and (department in ['hr']):
                    return department.upper()
            return match[0] if match else "Unknown"
        
        def clean_plan(value):
            valid_plan = ['dependent account', 'main account']
            if pd.isnull(value) or (str(value).strip() == ''):
                return None
            
            value = str(value).strip().lower()
            
            match = difflib.get_close_matches(value, valid_plan, n=1, cutoff=0.5)
            
            if match and len(match) > 0:
                return match[0].title()
            return match[0] if match else "Unknown"
        
        
        try:
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].apply(lambda x: unidecode(str(x)).strip().title() if pd.notnull(x) else x)
            
            df['user_id'] = df['user_id'].apply(clean_id)
            df['age'] = df['age'].apply(clean_age)
            df['sex'] = df['sex'].apply(clean_sex)
            df['nationality'] = df['nationality'].apply(clean_nationality)
            df['country_of_work'] = df['country_of_work'].apply(clean_nationality)
            df['ethnicity'] = df['ethnicity'].apply(clean_ethnicity)
            df['department'] = df['department'].apply(clean_department)
            df['plan'] = df['plan'].apply(clean_plan)
            df['age_bin'] = df['age'].astype('string').fillna('') + '~' + df['age_bin'].astype('string').fillna('')
            df['age_bin'] = df['age_bin'].apply(clean_age_bin)
        
            df['user_id'] = df['user_id'].astype('Int64')
            df['age'] = df['age'].astype('Int64')
            df['sex'] = df['sex'].astype('string')
            df['nationality'] = df['nationality'].astype('string')
            df['country_of_work'] = df['country_of_work'].astype('string')
            df['ethnicity'] = df['ethnicity'].astype('string')
            df['plan'] = df['plan'].astype('string')
            df['department'] = df['department'].astype('string')

            return df[cols], True
        
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return None, False

    def load_data_to_db(connection_str:str=None, df:pd.DataFrame=None, table_name:str=None):
        try:
            if (connection_str is None) or (df is None) or (table_name is None):
                return False
            engine = create_engine(connection_str)
            
            df.to_sql(table_name, engine, if_exists='append', index=False)
            return True
        
        except Exception as e:
            return False
        
        
    df, is_success = read_data()
    if not is_success:
        return False
    df, is_success = clean_data(df)
    if not is_success:
        return False
    
    is_success = load_data_to_db(
        connection_str=connection_str
        , df=df
        , table_name='stg_user'
    )
    if not is_success:
        return False
    
    return True
    
def import_event_data():
    import pandas as pd
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    from unidecode import unidecode
    
    data_prefix_path = '/opt/airflow/data'
    cols_dict = {
        'event_id': 'Int64'
        , 'event_name': 'string'
        , 'num_slots': 'Int64'
        , 'event_registered_date': 'string'
        , 'event_started_date': 'string'
        , 'event_end_date': 'string'
        , 'event_status': 'string'
    }
    
    # cast_cols = {col: 'string' for col in cols_dict.keys()}
    cols = [col for col in cols_dict.keys()]
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    def read_data():
        try: 
            df = pd.read_csv(f'{data_prefix_path}/events_data.csv', header=0, encoding='utf-8')
            df = df.replace(np.nan, '', regex=True)
            
            cast_cols = {col: 'string' for col in df.columns}
            df = df.astype(cast_cols)
            return df, True 
        
        except Exception as e:
            print(f"Error reading data: {e}")
            return None, False
    
    
    def clean_data(df):
        def clean_id(value):
            try:
                if pd.isna(value) or (str(value).strip() == ''):
                    return None
                
                if float(value) == int(float(value)):
                    return int(float(value))
                return -1
            
            except:
                return -1
 
        def clean_num_slots(value):
            try:
                if pd.isna(value) or str(value).strip() == '':
                    return -1
                num = int(float(value))
                return num
            except:
                return -1

        
        try:
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].apply(lambda x: unidecode(str(x)).strip().title() if pd.notnull(x) else x)
          
            df['event_id'] = df['event_id'].apply(clean_id)
            df['num_slots'] = df['num_slots'].apply(clean_num_slots)
            
            df['event_id'] = df['event_id'].astype('Int64')
            df['num_slots'] = df['num_slots'].astype('Int64')
            
            # df.to_csv(f'{data_prefix_path}/cleaned_users_data.csv', index=False, encoding='utf-8')
            return df, True
        
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return None, False

    def load_data_to_db(connection_str:str=None, df:pd.DataFrame=None, table_name:str=None):
        try:
            if (connection_str is None) or (df is None) or (table_name is None):
                return False
            engine = create_engine(connection_str)
            
            df.to_sql(table_name, engine, if_exists='append', index=False)
            return True
        
        except Exception as e:
            return False
        
        
    df, is_success = read_data()
    if not is_success:
        return False
    df, is_success = clean_data(df)
    if not is_success:
        return False
    
    # print(df.head(10))
    is_success = load_data_to_db(
        connection_str=connection_str
        , df=df
        , table_name='stg_event'
    )
    if not is_success:
        return False
    
    return True
     
def import_user_status_data():
    import pandas as pd
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    from unidecode import unidecode
    
    data_prefix_path = '/opt/airflow/data'
    cols_dict = {
        'user_id': 'Int64'
        , 'user_status': 'string'
        , 'created_date': 'string'
        , 'updated_date': 'string'
    }
    
    cols = [col for col in cols_dict.keys()]
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    def read_data():
        try: 
            df = pd.read_csv(f'{data_prefix_path}/user_journey_messy.csv', header=0, encoding='utf-8')
            df = df.replace(np.nan, '', regex=True)
            cast_cols = {col: 'string' for col in df.columns}
            
            df = df.astype(cast_cols)
            return df, True 
        
        except Exception as e:
            print(f"Error reading data: {e}")
            return None, False
    
    def clean_data(df):
        def clean_id(value):
            try:
                if pd.isna(value) or (str(value).strip() == ''):
                    return None
                # print(value)
                if float(value) == int(float(value)):
                    return int(float(value))
                return -1
            
            except:
                return -1
        
        def clean_status(value):
            if pd.isna(value) or (str(value).strip() == ''):
                return None
            return value.strip().title()
        
        try:
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].apply(lambda x: unidecode(str(x)).strip().title() if pd.notnull(x) else x)

            df = df.rename(columns={
                'UserID': 'user_id'
                , 'Status': 'user_status'
                , 'created_dt': 'created_date'
                , 'last_updated': 'updated_date'
            })
            
            df['user_id'] = df['user_id'].apply(clean_id)
            df['user_status'] = df['user_status'].apply(clean_status)
            
            df['user_id'] = df['user_id'].astype('Int64')
            df['user_status'] = df['user_status'].astype('string')
            
            # df.to_csv(f'{data_prefix_path}/cleaned_users_data.csv', index=False, encoding='utf-8')
            return df, True
        
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return None, False

    def load_data_to_db(connection_str:str=None, df:pd.DataFrame=None, table_name:str=None):
        try:
            if (connection_str is None) or (df is None) or (table_name is None):
                return False
            engine = create_engine(connection_str)
            
            df.to_sql(table_name, engine, if_exists='append', index=False)
            return True
        
        except Exception as e:
            print(f"Error loading data to DB: {e}")
            return False
        
        
    df, is_success = read_data()
    if not is_success:
        return False
    df, is_success = clean_data(df)
    if not is_success:
        return False
    
    # print(df.head(10))
    is_success = load_data_to_db(
        connection_str=connection_str
        , df=df
        , table_name='stg_user_status'
    )
    if not is_success:
        return False
    
    return True
  
def import_life_style_log_data():
    import pandas as pd
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    from unidecode import unidecode
    
    data_prefix_path = '/opt/airflow/data'
    cols_dict = {
        'user_id': 'Int64'
        , 'log_type': 'string'
        , 'log_date': 'string'
        , 'updated_date': 'string'
        , 'value': 'Int64'
        , 'unit': 'string'
    }
    
    
    cols = [col for col in cols_dict.keys()]
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    def read_data():
        try: 
            df = pd.read_csv(f'{data_prefix_path}/lifestyle_metrics.csv', header=0, encoding='utf-8')
            df = df.replace(np.nan, '', regex=True)
            cast_cols = {col: 'string' for col in df.columns}
            
            df = df.astype(cast_cols)
            return df, True 
        
        except Exception as e:
            print(f"Error reading data: {e}")
            return None, False
      
    def clean_data(df):
        def clean_id(value):
            try:
                if pd.isna(value) or (str(value).strip() == ''):
                    return None
                
                if float(value) == int(float(value)):
                    return int(float(value))
                return -1
            
            except:
                return -1
 
        def clean_value(value):
            try:
                if pd.isna(value) or str(value).strip() == '':
                    return None
                num = int(float(value))
                return num
            except:
                return -1

        
        
        try:
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].apply(lambda x: unidecode(str(x)).strip().title() if pd.notnull(x) else x)

            df['user_id'] = df['user_id'].apply(clean_id)
            df['user_id'] = df['user_id'].astype('Int64')
            
            df['value'] = df['value'].apply(clean_value)
            df['value'] = df['value'].astype('Int64')
            
            
            # df.to_csv(f'{data_prefix_path}/cleaned_users_data.csv', index=False, encoding='utf-8')
            return df, True
        
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return None, False

    def load_data_to_db(connection_str:str=None, df:pd.DataFrame=None, table_name:str=None):
        try:
            if (connection_str is None) or (df is None) or (table_name is None):
                return False
            engine = create_engine(connection_str)
            
            df.to_sql(table_name, engine, if_exists='append', index=False)
            return True
        
        except Exception as e:
            return False
        
        
    df, is_success = read_data()
    if not is_success:
        return False
    df, is_success = clean_data(df)
    if not is_success:
        return False
    
    # print(df.head(10))
    is_success = load_data_to_db(
        connection_str=connection_str
        , df=df
        , table_name='stg_lifestyle_log'
    )
    if not is_success:
        return False
    
    return True
  
def import_mindfulness_scores_data():
    import pandas as pd
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    from unidecode import unidecode
    
    data_prefix_path = '/opt/airflow/data'
    cols_dict = {
        'user_id': 'Int64'
        , 'log_type': 'string'
        , 'log_date': 'string'
        , 'score': 'Int64'
        , 'score_band': 'string'
        , 'mindfulness_band': 'string'
    }
    
    cols = [col for col in cols_dict.keys()]
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    def read_data():
        try: 
            df = pd.read_csv(f'{data_prefix_path}/mindfulness_scores.csv', header=0, encoding='utf-8')
            df = df.replace(np.nan, '', regex=True)
            cast_cols = {col: 'string' for col in df.columns}
            
            df = df.astype(cast_cols)
            return df, True 
        
        except Exception as e:
            print(f"Error reading data: {e}")
            return None, False
    
    def clean_data(df):
        def clean_id(value):
            try:
                if pd.isna(value) or (str(value).strip() == ''):
                    return 0
                
                if float(value) == int(float(value)):
                    return int(float(value))
                return None
            
            except:
                return -1
    
        def clean_value(value):
            try:
                if pd.isna(value) or str(value).strip() == '':
                    return -1
                num = int(float(value))
                return num
            except:
                return -1
    
        try:
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].apply(lambda x: unidecode(str(x)).strip().title() if pd.notnull(x) else x)

            df['user_id'] = df['user_id'].apply(clean_id)
            df['user_id'] = df['user_id'].astype('Int64')
            
            df['score'] = df['score'].apply(clean_value)
            df['score'] = df['score'].astype('Int64')
            
            # df.to_csv(f'{data_prefix_path}/cleaned_users_data.csv', index=False, encoding='utf-8')
            return df, True
        
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return None, False

    def load_data_to_db(connection_str:str=None, df:pd.DataFrame=None, table_name:str=None):
        try:
            if (connection_str is None) or (df is None) or (table_name is None):
                return False
            engine = create_engine(connection_str)
            
            df.to_sql(table_name, engine, if_exists='append', index=False)
            return True
        
        except Exception as e:
            return False
         
    df, is_success = read_data()
    if not is_success:
        return False
    df, is_success = clean_data(df)
    if not is_success:
        return False
    
    # print(df.head(10))
    is_success = load_data_to_db(
        connection_str=connection_str
        , df=df
        , table_name='stg_mindfulness_scores'
    )
    if not is_success:
        return False
    
    return True
    
def import_user_activity_data():
    import pandas as pd
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    from unidecode import unidecode
    import difflib
    
    data_prefix_path = '/opt/airflow/data'
    cols_dict = {
        'user_id': 'Int64'
        , 'event_id': 'Int64'
        , 'user_status': 'string'
        , 'status_updated_date': 'string'
    }
    
    cols = [col for col in cols_dict.keys()]
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    def read_data():
        try: 
            df = pd.read_csv(f'{data_prefix_path}/activity_participation.csv', header=0, encoding='utf-8')
            df = df.replace(np.nan, '', regex=True)
            cast_cols = {col: 'string' for col in df.columns}
            df = df.astype(cast_cols)
            
            return df, True 
        
        except Exception as e:
            print(f"Error reading data: {e}")
            return None, False
    
    def clean_id(value):
        try:
            if pd.isna(value) or (str(value).strip() == ''):
                return 0
            # print(value)
            if float(value) == int(float(value)):
                return int(float(value))
            return -1
        
        except:
            return -1
    
    def clean_status(value):
        if pd.isna(value) or (str(value).strip() == ''):
            return None
        
        valid_sexes = ['cancelled', 'participated', 'joined']
        if pd.isnull(value) or (str(value).strip() == ''):
            return None
        value = str(value).strip().title().lower()
        
        match = difflib.get_close_matches(value, valid_sexes, n=1, cutoff=0.5)
        return match[0].lower().title() if match else "Unknown"
    
    def clean_data(df):
        try:
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].apply(lambda x: unidecode(str(x)).strip().title() if pd.notnull(x) else x)
            
            df = df.rename(columns={
                'UserID': 'user_id'
                , 'event_id': 'event_id'
                , 'Status': 'user_status'
                , 'status_updated_date': 'status_updated_date'
            })
            
            # print(df.columns)
            df['user_id'] = df['user_id'].apply(clean_id)
            df['user_id'] = df['user_id'].astype('Int64')
            
            df['event_id'] = df['event_id'].apply(clean_id)
            df['event_id'] = df['event_id'].astype('Int64')
            
            df['user_status'] = df['user_status'].apply(clean_status)
            df['user_status'] = df['user_status'].astype('string')
            
            
            # df.to_csv(f'{data_prefix_path}/cleaned_users_data.csv', index=False, encoding='utf-8')
            return df, True
        
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return None, False

    def load_data_to_db(connection_str:str=None, df:pd.DataFrame=None, table_name:str=None):
        try:
            if (connection_str is None) or (df is None) or (table_name is None):
                return False
            engine = create_engine(connection_str)
            
            df.to_sql(table_name, engine, if_exists='append', index=False)
            return True
        
        except Exception as e:
            print(f"Error loading data to DB: {e}")
            return False
        
        
    df, is_success = read_data()
    if not is_success:
        return False
    df, is_success = clean_data(df)
    if not is_success:
        return False
    
    # print(df.head(10))
    is_success = load_data_to_db(
        connection_str=connection_str
        , df=df
        , table_name='stg_user_activity'
    )
    if not is_success:
        return False
    
    return True

def import_health_log_data():
    import pandas as pd
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    from unidecode import unidecode
    
    data_prefix_path = '/opt/airflow/data'
    cols_dict = {
        'user_id': 'Int64'
        , 'log_type': 'string'
        , 'log_date': 'string'
        , 'value': 'float'
        , 'value_band': 'string'
        , 'unit': 'string'
        , 'health_index': 'float'
    }
    cols = [col for col in cols_dict.keys()]
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    def read_data():
        try: 
            df = pd.read_csv(f'{data_prefix_path}/health_measurements.csv', header=0, encoding='utf-8')
            df = df.replace(np.nan, '', regex=True)
            cast_cols = {col: 'string' for col in df.columns}
            df = df.astype(cast_cols)
            
            return df, True 
        
        except Exception as e:
            print(f"Error reading data: {e}")
            return None, False
    
    def clean_data(df):
        def clean_id(value):
            try:
                if pd.isna(value) or (str(value).strip() == ''):
                    return None
                
                if float(value) == int(float(value)):
                    return int(float(value))
                return -1
            
            except:
                return -1
    
        def clean_value(value):
            try:
                if pd.isna(value) or str(value).strip() == '':
                    return None
                return float(value)
            
            except:
                return -1
    
        try:
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].apply(lambda x: unidecode(str(x)).strip().title() if pd.notnull(x) else x)
            
            # print(df.columns)
            df['user_id'] = df['user_id'].apply(clean_id)
            df['user_id'] = df['user_id'].astype('Int64')
            
            df['value'] = df['value'].apply(clean_value)
            df['value'] = df['value'].astype('float')
            df['health_index'] = df['health_index'].apply(clean_value)
            df['health_index'] = df['health_index'].astype('float')
            
            
            # df.to_csv(f'{data_prefix_path}/cleaned_users_data.csv', index=False, encoding='utf-8')
            return df, True
        
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return None, False

    def load_data_to_db(connection_str:str=None, df:pd.DataFrame=None, table_name:str=None):
        try:
            if (connection_str is None) or (df is None) or (table_name is None):
                return False
            engine = create_engine(connection_str)
            
            df.to_sql(table_name, engine, if_exists='append', index=False)
            return True
        
        except Exception as e:
            return False
        
        
    df, is_success = read_data()
    if not is_success:
        return False
    df, is_success = clean_data(df)
    if not is_success:
        return False
    
    # print(df.head(10))
    is_success = load_data_to_db(
        connection_str=connection_str
        , df=df
        , table_name='stg_health_log'
    )
    if not is_success:
        return False
    
    return True



def dedup_user_data():
    from sqlalchemy import create_engine
    import pandas as pd
    
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    try:
        engine = create_engine(connection_str)
        query = """
            WITH ranked_data AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER (PARTITION BY user_id) AS rn
                FROM stg_user
            )
            SELECT
                user_id
                , sex
                , plan
                , age
                , age_bin
                , ethnicity
                , department
                , nationality
                , country_of_work
            from ranked_data
            WHERE rn = 1
        """
        df = pd.read_sql(query, engine)
        
        df.to_sql('dwh_user', engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error deduplicating data: {e}")
        return False

def dedup_event_data():
    from sqlalchemy import create_engine
    import pandas as pd
    
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    try:
        engine = create_engine(connection_str)
        query = """
            WITH ranked_data AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER (PARTITION BY event_id) AS rn
                FROM stg_event
            )
            SELECT
                event_id
                , event_name
                , num_slots
                , event_registered_date
                , event_started_date
                , event_end_date
                , event_status
            from ranked_data
            WHERE rn = 1
        """
        df = pd.read_sql(query, engine)
        
        df.to_sql('dwh_event', engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error deduplicating data: {e}")
        return False

def dedup_user_status_data():
    from sqlalchemy import create_engine
    import pandas as pd
    
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    try:
        engine = create_engine(connection_str)
        query = """
            WITH ranked_data AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_date DESC) AS rn
                FROM stg_user_status
            )
            SELECT
                user_id
                , user_status
                , created_date
                , updated_date
            from ranked_data
            WHERE rn = 1
        """
        df = pd.read_sql(query, engine)
        df.to_sql('dwh_user_status', engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error deduplicating data: {e}")
        return False
    
def dedup_lifestyle_log_data():
    from sqlalchemy import create_engine
    import pandas as pd
    
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    try:
        engine = create_engine(connection_str)
        query = """
            WITH ranked_data AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER (PARTITION BY user_id, log_type ORDER BY log_date DESC) AS rn
                FROM stg_lifestyle_log
            )
            SELECT
                user_id
                , log_type
                , log_date
                , value
                , unit
            from ranked_data
            WHERE rn = 1
        """
        df = pd.read_sql(query, engine)
        df.to_sql('dwh_lifestyle_log', engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error deduplicating data: {e}")
        return False  
    
def dedup_mindfulness_scores_data():
    from sqlalchemy import create_engine
    import pandas as pd
    
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    try:
        engine = create_engine(connection_str)
        query = """
            WITH ranked_data AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER (PARTITION BY user_id, log_type ORDER BY log_date DESC) AS rn
                FROM stg_mindfulness_scores
            )
            SELECT
                user_id
                , log_type
                , log_date
                , score
                , score_band
                , mindfulness_band
            from ranked_data
            WHERE rn = 1
        """
        df = pd.read_sql(query, engine)
        df.to_sql('dwh_mindfulness_scores', engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error deduplicating data: {e}")
        return False 
    
def dedup_user_activity_data():
    from sqlalchemy import create_engine
    import pandas as pd
    
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    try:
        engine = create_engine(connection_str)
        query = """
            WITH ranked_data AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER (PARTITION BY user_id, event_id ORDER BY status_updated_date DESC) AS rn
                FROM stg_user_activity
            )
            SELECT
                user_id
                , event_id
                , user_status
                , status_updated_date
            from ranked_data
            WHERE rn = 1
        """
        df = pd.read_sql(query, engine)
        df.to_sql('dwh_user_activity', engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error deduplicating data: {e}")
        return False

def dedup_health_log_data():
    from sqlalchemy import create_engine
    import pandas as pd
    
    connection_str = 'postgresql://airflow:airflow@postgres-airflow:5432/health_tracking'
    
    try:
        engine = create_engine(connection_str)
        query = """
            WITH ranked_data AS (
                SELECT 
                    *
                    , ROW_NUMBER() OVER (PARTITION BY user_id, log_type ORDER BY log_date DESC) AS rn
                FROM stg_health_log
            )
            SELECT
                user_id
                , log_type
                , log_date
                , value
                , value_band
                , unit
                , health_index
            from ranked_data
            WHERE rn = 1
        """
        df = pd.read_sql(query, engine)
        df.to_sql('dwh_health_log', engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error deduplicating data: {e}")
        return False




    
dag_list = [
    {
        'dag_id': 'import_user_data'
        , 'next_triggered_dag': 'dedup_user_data'
        , 'function': import_user_data
        , 'schedule_interval': '0 0 * * *'
    }
    , {
        'dag_id': 'import_event_data'
        , 'next_triggered_dag': 'dedup_event_data'
        , 'function': import_event_data
        , 'schedule_interval': '0 0 * * *'
    }
    , {
        'dag_id': 'import_user_status_data'
        , 'next_triggered_dag': 'dedup_user_status_data'
        , 'function': import_user_status_data
        , 'schedule_interval': '0 0 * * *'
    }
    , {
        'dag_id': 'import_life_style_log_data'
        , 'next_triggered_dag': 'dedup_lifestyle_log_data'
        , 'function': import_life_style_log_data
        , 'schedule_interval': '0 0 * * *'
    }
    , {
        'dag_id': 'import_mindfulness_scores_data'
        , 'next_triggered_dag': 'dedup_mindfulness_scores_data'
        , 'function': import_mindfulness_scores_data
        , 'schedule_interval': '0 0 * * *'
    }
    , {
        'dag_id': 'import_user_activity_data'
        , 'next_triggered_dag': 'dedup_user_activity_data'
        , 'function': import_user_activity_data
        , 'schedule_interval': '0 0 * * *'
    }
    , {
        'dag_id': 'dedup_user_data'
        , 'function': dedup_user_data
        , 'schedule_interval': '30 0 * * *'
    }
    , {
        'dag_id': 'dedup_event_data'
        , 'function': dedup_event_data
        , 'schedule_interval': '30 0 * * *'
    }
    , {
        'dag_id': 'dedup_user_status_data'
        , 'function': dedup_user_status_data
        , 'schedule_interval': '30 0 * * *'
    }
    , {
        'dag_id': 'dedup_lifestyle_log_data'
        , 'function': dedup_lifestyle_log_data
        , 'schedule_interval': '30 0 * * *'
    }
    , {
        'dag_id': 'dedup_mindfulness_scores_data'
        , 'function': dedup_mindfulness_scores_data
        , 'schedule_interval': '30 0 * * *'
    }
    , {
        'dag_id': 'dedup_user_activity_data'
        , 'function': dedup_user_activity_data
        , 'schedule_interval': '30 0 * * *'
    }
    , {
        'dag_id': 'dedup_health_log_data'
        , 'function': dedup_health_log_data
        , 'schedule_interval': '30 0 * * *'
    }
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

for dag_info in dag_list:
    dag_id = dag_info['dag_id']
    function = dag_info['function']
    
    with DAG(
        dag_id
        , default_args=default_args
        , schedule_interval=dag_info.get('schedule_interval', None)
    ) as dag:
        task = PythonOperator(
            task_id=dag_id
            , python_callable=import_user_data
            , dag=dag
        )
        task



