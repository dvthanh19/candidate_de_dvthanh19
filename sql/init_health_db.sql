-- Create the health_tracking database for the data pipeline
CREATE DATABASE health_tracking;

-- Grant permissions to airflow user
GRANT ALL PRIVILEGES ON DATABASE health_tracking TO airflow;