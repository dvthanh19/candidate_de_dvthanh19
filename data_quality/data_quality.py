
import pandas as pd
import numpy as np
import os
import json

data_folder = '../data' if not os.path.exists('./data') else './data'
files = [
    'users_data.csv',
    'activity_participation.csv',
    'events_data.csv',
    'health_measurements.csv',
    'lifestyle_metrics.csv',
    'mindfulness_scores.csv',
    'user_journey_messy.csv',
]

def check_users_data(df):
    errors = {}
    error_rows = set()
    # user_id: not null, unique, positive integer
    null_mask = df['user_id'].isnull()
    errors['user_id_missing'] = null_mask.sum()
    error_rows.update(df[null_mask].index.tolist())
    # Only check other errors if not null
    valid_mask = ~null_mask
    errors['user_id_duplicates'] = df.loc[valid_mask, 'user_id'].duplicated().sum()
    errors['user_id_negative'] = (df.loc[valid_mask, 'user_id'] <= 0).sum()
    error_rows.update(df.loc[valid_mask][df.loc[valid_mask, 'user_id'] <= 0].index.tolist())
    # sex: allowed values
    null_mask = df['sex'].isnull()
    errors['sex_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    valid_sex = ['Male', 'Female']
    invalid_mask = ~df.loc[valid_mask, 'sex'].isin(valid_sex)
    errors['sex_invalid'] = invalid_mask.sum()
    error_rows.update(df.loc[valid_mask][invalid_mask].index.tolist())
    # age: not null, positive
    null_mask = df['age'].isnull()
    errors['age_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['age_negative'] = (df.loc[valid_mask, 'age'] < 0).sum()
    error_rows.update(df.loc[valid_mask][df.loc[valid_mask, 'age'] < 0].index.tolist())
    # plan: allowed values
    null_mask = df['plan'].isnull()
    errors['plan_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    valid_plan = ['Main Account', 'Dependent Account']
    invalid_mask = ~df.loc[valid_mask, 'plan'].isin(valid_plan)
    errors['plan_invalid'] = invalid_mask.sum()
    error_rows.update(df.loc[valid_mask][invalid_mask].index.tolist())
    # department: allowed values
    null_mask = df['department'].isnull()
    errors['department_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    valid_dept = ['Business', 'HR', 'Product']
    invalid_mask = ~df.loc[valid_mask, 'department'].isin(valid_dept)
    errors['department_invalid'] = invalid_mask.sum()
    error_rows.update(df.loc[valid_mask][invalid_mask].index.tolist())
    # ethnicity: allowed values
    null_mask = df['ethnicity'].isnull()
    errors['ethnicity_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    valid_ethnicity = ['Chinese', 'Malay', 'Indian', 'Others']
    invalid_mask = ~df.loc[valid_mask, 'ethnicity'].isin(valid_ethnicity)
    errors['ethnicity_invalid'] = invalid_mask.sum()
    error_rows.update(df.loc[valid_mask][invalid_mask].index.tolist())
    return errors, error_rows

def check_activity_participation(df):
    errors = {}
    error_rows = set()
    
    null_mask = df['UserID'].isnull()
    errors['user_id_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['user_id_negative'] = (df.loc[valid_mask, 'UserID'] <= 0).sum()
    error_rows.update(df[null_mask | (df['UserID'] <= 0)].index.tolist())

    null_mask = df['event_id'].isnull()
    errors['event_id_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['event_id_negative'] = (df.loc[valid_mask, 'event_id'] <= 0).sum()
    error_rows.update(df[null_mask | (df['event_id'] <= 0)].index.tolist())

    null_mask = df['Status'].isnull()
    errors['user_status_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    valid_status = ['Cancelled', 'Participated', 'Joined']
    invalid_mask = ~df.loc[valid_mask, 'Status'].isin(valid_status)
    errors['user_status_invalid'] = invalid_mask.sum()
    error_rows.update(df.loc[valid_mask][invalid_mask].index.tolist())
    return errors, error_rows

def check_events_data(df):
    errors = {}
    error_rows = set()
    
    null_mask = df['event_id'].isnull()
    errors['event_id_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['event_id_negative'] = (df.loc[valid_mask, 'event_id'] <= 0).sum()
    error_rows.update(df[null_mask | (df['event_id'] <= 0)].index.tolist())

    null_mask = df['event_name'].isnull()
    errors['event_name_missing'] = null_mask.sum()
    error_rows.update(df[null_mask].index.tolist())

    null_mask = df['num_slots'].isnull()
    errors['num_slots_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['num_slots_negative'] = (df.loc[valid_mask, 'num_slots'] < 0).sum()
    error_rows.update(df[null_mask | (df['num_slots'] < 0)].index.tolist())
    return errors, error_rows

def check_health_measurements(df):
    errors = {}
    error_rows = set()
    
    null_mask = df['user_id'].isnull()
    errors['user_id_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['user_id_negative'] = (df.loc[valid_mask, 'user_id'] <= 0).sum()
    error_rows.update(df[null_mask | (df['user_id'] <= 0)].index.tolist())

    null_mask = df['value'].isnull()
    errors['value_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['value_negative'] = (df.loc[valid_mask, 'value'] < 0).sum()
    error_rows.update(df[null_mask | (df['value'] < 0)].index.tolist())

    null_mask = df['health_index'].isnull()
    errors['health_index_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['health_index_negative'] = (df.loc[valid_mask, 'health_index'] < 0).sum()
    error_rows.update(df[null_mask | (df['health_index'] < 0)].index.tolist())
    return errors, error_rows

def check_lifestyle_metrics(df):
    errors = {}
    error_rows = set()
    
    null_mask = df['user_id'].isnull()
    errors['user_id_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['user_id_negative'] = (df.loc[valid_mask, 'user_id'] <= 0).sum()
    error_rows.update(df[null_mask | (df['user_id'] <= 0)].index.tolist())

    null_mask = df['log_type'].isnull()
    errors['log_type_missing'] = null_mask.sum()
    error_rows.update(df[null_mask].index.tolist())

    null_mask = df['value'].isnull()
    errors['value_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['value_negative'] = (df.loc[valid_mask, 'value'] < 0).sum()
    error_rows.update(df[null_mask | (df['value'] < 0)].index.tolist())
    return errors, error_rows

def check_mindfulness_scores(df):
    errors = {}
    error_rows = set()
    
    null_mask = df['user_id'].isnull()
    errors['user_id_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['user_id_negative'] = (df.loc[valid_mask, 'user_id'] <= 0).sum()
    error_rows.update(df[null_mask | (df['user_id'] <= 0)].index.tolist())

    null_mask = df['score'].isnull()
    errors['score_missing'] = null_mask.sum()
    valid_mask = ~null_mask
    errors['score_negative'] = (df.loc[valid_mask, 'score'] < 0).sum()
    error_rows.update(df[null_mask | (df['score'] < 0)].index.tolist())
    return errors, error_rows

def check_user_journey_messy(df):
    errors = {}
    error_rows = set()
    
    null_mask = df['UserID'].isnull() if 'UserID' in df.columns else pd.Series([False]*len(df))
    errors['user_id_missing'] = null_mask.sum()
    error_rows.update(df[null_mask].index.tolist())

    null_mask = df['Status'].isnull() if 'Status' in df.columns else pd.Series([False]*len(df))
    errors['status_missing'] = null_mask.sum()
    error_rows.update(df[null_mask].index.tolist())
    return errors, error_rows

custom_checks = {
    'users_data.csv': check_users_data,
    'activity_participation.csv': check_activity_participation,
    'events_data.csv': check_events_data,
    'health_measurements.csv': check_health_measurements,
    'lifestyle_metrics.csv': check_lifestyle_metrics,
    'mindfulness_scores.csv': check_mindfulness_scores,
    'user_journey_messy.csv': check_user_journey_messy,
}

def check_data_quality(file):
    path = os.path.join(data_folder, file)
    df = pd.read_csv(path)
    print(f'\n--- Data Quality Report for {file} ---')
    print(f'Shape: {df.shape}')
    errors, error_rows = custom_checks[file](df)
    
    print('Error counts per column:')
    print(f'{json.dumps(errors, default=lambda x: int(x) if isinstance(x, np.integer) else x, indent=2)}' )
    
    print(f'Number of error rows: {len(error_rows)}')
    print('-----------------------------------')

for file in files:
    try:
        check_data_quality(file)
    except Exception as e:
        print(f'Error processing {file}: {e}')

