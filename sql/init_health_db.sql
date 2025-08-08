-- Create the health_tracking database for the data pipeline
create database health_tracking;

-- Grant permissions to airflow user
grant all privileges on database health_tracking to airflow;
