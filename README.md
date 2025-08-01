# Data Engineer Test Instructions

Hello and thanks for your interest in our company! 🙃

This task is designed to test your technical proficiency with data engineering, Python and containers.

**You needn't complete the entire test but the more you complete, the easier it will be for us to get a good idea of your skill set.**

This assessment has a **strict deadline**. You must submit your work **before the deadline** as your access token will expire shortly after.

❗ **Important**: If you haven't already, please read **READ_FIRST.md** for GitLab access setup instructions before starting.

**Getting Started:**
- Complete all work in your own branch: `candidate/[your-name]`
- Comments are welcome in the **Additional Notes** section of your `ANSWERS.md`
- When ready to submit, push your branch to the repository
- Email to HR to notify about your submission

Send us questions if you have any. We're looking forward to reviewing your task. Good luck! 🚀

# Health Data ETL Pipeline Challenge
The primary purpose of this test is to build a robust data pipeline that processes health tracking data and answers specific business questions.

💡 _Hint_: Reading through the tasks, you might decide to change your approach. So, it's a good idea to **read the entire document before you start**.

Feel free to write the data pipeline in pure Python code or use a framework such as [dbt](https://docs.getdbt.com/) or [Apache Airflow](https://airflow.apache.org/).

0. **Business Questions** 🤔

- **Event Participation Analysis**: Calculate participation rates across different health events and identify the most popular activities. Which events have the highest participation rates?
- **Daily Lifestyle Metrics**: What are the average daily values for each lifestyle metric (steps, sleep, MVPA) per user demographic group?
- **Data Quality Issues**: Identify and report on data quality issues in the provided datasets. Which files have the most missing values or inconsistencies?

Once you've developed and run your pipeline, please explore the resulting data and provide your answers in a separate `ANSWERS.md` file.

1. **Set up your development environment**
We've provided a complete Docker environment for you. Simply run:
```bash
docker-compose up -d --build
```
This will start:
- PostgreSQL database with the `health_tracking` database ready for your data
- Apache Airflow with web UI accessible at http://localhost:8080

2. **Create your data pipeline**
Build an Apache Airflow DAG that performs ETL operations on the health tracking data:

- **Extract**: Read CSV files from the `/opt/airflow/data/` directory
- **Transform**: Clean and process the data according to the business requirements
- **Load**: Insert the transformed data into PostgreSQL tables

Your pipeline should:
- Handle data quality issues (missing values, duplicates, format inconsistencies)
- Create appropriate database schema based on the provided ERD
- Be structured in a way that is easily extensible and maintainable
- Support repeatability and idempotent operations

3. **Answer business questions**
Design your data transformations to enable answering the business questions. Create appropriate tables, views, or aggregations that support the required analysis.

**Additional considerations**
When we assess your work, we're also going to consider:

✅ How credentials are managed and how easily the pipeline can be deployed in different environments (dev, test, prod);

✅ If the git commit messages show a good understanding of what has happened during development;

✅ Code quality, documentation, and testing approach;

✅ Data pipeline design and scalability considerations.

# Getting Started

## Prerequisites
- Docker and Docker Compose installed on your machine
- At least 4GB RAM and 2 CPUs available for Docker

## Repository Structure
```
health_data_pipeline/
├── dags/
│   ├── health_etl/
│   │   ├── __init__.py
│   │   ├── dag.py
│   │   └── helpers.py
├── data/
│   ├── users_data.csv
│   ├── user_journey_messy.csv
│   ├── events_data.csv
│   ├── activity_participation.csv
│   ├── lifestyle_metrics.csv
│   ├── mindfulness_scores.csv
│   ├── health_measurements.csv
│   └── erd_chart.png
├── sql/
│   └── create_tables.sql
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── README.md
└── ANSWERS.md
```

## Provided Docker Setup

We provide a complete Docker environment including:
- **Dockerfile**: Based on `apache/airflow:2.10.5-python3.12` with all necessary dependencies
- **docker-compose.yml**: Full orchestration with Airflow and PostgreSQL services
- **requirements.txt**: Pre-configured Python packages for data processing
- **sql/init_health_db.sql**: Database initialization script

You can modify these files as needed for your implementation.

## Environment Setup
```bash
# Clone your repository and navigate to the project directory
cd your-project-directory

# Build and start all services (this will take 5-10 minutes)
docker-compose up -d --build

# Check if services are running
docker-compose ps
```

## Access Services

1. **Airflow Web UI**: [http://localhost:8080](http://localhost:8080)
   ```
   Username: airflow
   Password: airflow
   ```

2. **PostgreSQL Database**: Available at `localhost:5432`
   ```
   Host: localhost
   Port: 5432
   Database: health_tracking (for your data pipeline)
   Database: airflow (for Airflow metadata)
   Username: airflow
   Password: airflow
   ```

## Development Workflow

### Step 1: Develop Your Pipeline
1. Create your DAG files in the `dags/` directory
2. The CSV data files are available in `/opt/airflow/data/` within the container
3. Use the ERD chart (`data/erd_chart.png`) to understand the expected database schema
4. SQL scripts can be placed in the `sql/` directory

### Step 2: Run Your Pipeline
1. Navigate to the Airflow UI at [http://localhost:8080](http://localhost:8080)
2. Find your DAG in the DAGs list
3. Toggle the DAG to "On" (unpause it)
4. Trigger the DAG manually using the "Trigger DAG" button
5. Monitor pipeline execution through the Graph View and Logs

### Step 3: Verify Results
- Connect to PostgreSQL to check your transformed data
- Use any PostgreSQL client or connect via command line:
  ```bash
  docker-compose exec postgres psql -U airflow -d health_tracking
  ```
- Answer the business questions in the `ANSWERS.md` file

## Troubleshooting
- If containers fail to start, check Docker logs: `docker-compose logs`
- Ensure you have sufficient system resources (4GB RAM minimum)
- Wait for PostgreSQL health checks to pass before Airflow starts

# Submission Requirements

After completing your pipeline, ensure your repository includes:
1. All source code for your ETL pipeline (DAGs, helper functions)
2. Updated `ANSWERS.md` with your business question responses
3. Any custom SQL scripts or schema definitions
4. Clear git commit history showing your development process


---

## Dataset Information

### Files Description
- `users_data.csv` - User demographic and profile information
- `user_journey_messy.csv` - User enrollment and status changes over time  
- `events_data.csv` - Health and fitness events offered to users
- `activity_participation.csv` - User participation in various activities
- `lifestyle_metrics.csv` - Daily lifestyle tracking (steps, sleep, MVPA)
- `mindfulness_scores.csv` - Mental wellbeing survey responses
- `health_measurements.csv` - Physical health measurements and biomarkers
- `erd_chart.png` - Entity Relationship Diagram showing the database schema and table relationships

### Data Quality Issues to Expect
- Missing values and inconsistent data formats
- Duplicate records with slight variations
- Inconsistent column naming across files
- Data entry errors and typos
- Invalid values outside expected ranges
- Mixed units for the same measurements
- Encoding issues with special characters
- Potential outliers and incorrect data types

Good luck! This assignment is designed to test your practical data engineering skills.
