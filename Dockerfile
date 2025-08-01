FROM apache/airflow:2.10.5-python3.12

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and related files
COPY dags/ /opt/airflow/dags/
COPY data/ /opt/airflow/data/
COPY sql/ /opt/airflow/sql/

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True