# Business Questions Answers

After running your data pipeline, provide answers to the business questions in this section:

## Business Question 1: Event Participation Analysis
**Question:** Calculate participation rates across different health events and identify the most popular activities. Which events have the highest participation rates?

**Answer:** Here are the insights from the event participation analysis table:

- The events with the highest participation rates are:
  - **Ultimate Frisbee Frenzy @ Bu** (0.80)
  - **Singapore Skating Showdown** (0.78)  
--> These events filled a large proportion of their available slots.

- Larger events (**Morning Run @ East Coast Park** and **Beach Volleyball Bash @ Silos**) have more participants in absolute numbers, but lower participation rates due to higher slot counts.

 
=> The medium or small sized events possess the higher participation rate compared to the larger event, however the approached participants of the latter is about 3 times larger in the figure for participants.


**Method:** 
We have:  
- Table *event*: event information
- Table *user_activity*: user's activity information with dimension (FK: event_id)
- We need to get information about event so we use *event* as the main table to left join with *user_activity*

**SQL Query/Code Used:**
```sql
SELECT
    e.event_id
    , e.event_name
    , e.num_slots
    , COUNT(DISTINCT ua.user_id) AS num_participants
    , ROUND(COUNT(DISTINCT ua.user_id)::numeric / NULLIF(e.num_slots, 0), 2) AS participation_rate
FROM dwh_event AS e
LEFT JOIN dwh_user_activity AS ua 
    ON e.event_id = ua.event_id
    AND lower(ua.user_status) = 'participated'
GROUP BY e.event_id, e.event_name, e.num_slots
ORDER BY participation_rate DESC NULLS LAST, num_participants DESC;
```

---

## Business Question 2: Daily Lifestyle Metrics
**Question:** What are the average daily values for each lifestyle metric (steps, sleep, MVPA) per user demographic group?

**Answer:** 
- *Step* value totally dominate the other lifestyle metrics: *sleep* and *MVPA*, referenced from mostly all user demographic group.
- For all 3 metrics, men always have the higher value
- The highest daily value is 7548.75 which is belongs to the persona:
{
    log_type: step
    , sex: Female
    , age_bin: 60+
    , department: Product
    , ethnicity: Chinese
    , nationality: India
    , country_of_work: Singapore
}
- The lower daily value is 54.8 which is belongs to the persona:
{
    log_type: step
    , sex: Female
    , age_bin: 11-20
    , department: Product
    , ethnicity: Chinese
    , nationality: China
    , country_of_work: Singapore
}
- Besides, there can be many insight from this kind of data depend on the detail of the group.


**Method:** 
- First, I choose some demographic features to group: sex, age_bin, department, ethnicity, nationality, country_of_work
- Map the user with their lifestyle metric
- Group user in the same demographic group, and get the result
- I also group by for only each feature one by one to analysis


**SQL Query/Code Used:**
```sql
SELECT 
    l.log_type
    , u.sex
    , u.age_bin
    , u.department
    , u.ethnicity
    , u.nationality
    , u.country_of_work
    , ROUND(AVG(l.value), 2) AS avg_daily_value
FROM dwh_lifestyle_log AS l
LEFT JOIN dwh_user AS u 
    ON l.user_id = u.user_id
WHERE l.log_type IN ('step', 'sleep', 'mvpa')
    AND u.user_id IS NOT NULL
GROUP BY 
    l.log_type
    , u.sex
    , u.age_bin
    , u.ethnicity
    , u.department
    , u.nationality
    , u.country_of_work
ORDER BY ROUND(AVG(l.value), 2)
```

---

## Business Question 3: Data Quality Issues
**Question:** Identify and report on data quality issues in the provided datasets. Which files have the most missing values or inconsistencies?

**Answer:** Based on the data cleaning and loading process, the users_data.csv file has the most data quality issues. It contains a high number of missing values, inconsistencies (such as typos and duplicate records), and more rows and columns compared to other files. These issues required extensive cleaning, including handling missing values, standardizing categorical fields, and deduplicating records. Other files (such as event, activity, and log files) had fewer columns and less frequent inconsistencies.

**Method:**
- Reviewed each dataset during the cleaning process.
- Noted the frequency of missing values, typos, and duplicate records.
- Compared the number of rows and columns across files.

**SQL Query/Code Used:**
```sql
-- Please check the clean function in each dag
```

---

## Additional Notes

[Include any additional thoughts, challenges faced, design decisions, or areas for improvement]

### Pipeline Design Decisions
**Design:**
Components:  
- Pure Python for cleaning and loading data
- SQL to create table, view and analyse data
- Airflow for orchestration
Flow:  
    - All dag can be triggered at a time
    - Data will be stored into PosgreSQL
    - Query from view to get required data


**Explanation:**
- Due to my current company workload, I have not had much time to build a pipeline with dbt tool and develop it in a standard pipeline with high scalibility, monitoring, data quality, authentication, authorization
- The file can be considered as a lake storage or landing layer in a fully built architecture
- Data has many error iniside: missing, typos and duplication
    - Only small part of fields can be infered from other field (e.g: age -> age_bin)
    - Typo error is only can solved by code nor the sql.
    - Divide cleaning step to 2 small parts: fix error and solve missing data if possible + deuplicate data. Therefore, we can load data and then use SQL to deduplicate it (staging layer)

### Challenges Encountered
- The fundamental of this project is not clear for OLAP or OLTP
- It is hard to deduplicate for id due to missing monitoring columns: created_at, updated_at
Solution
- I assume it is a OLAP because the given data file (despite missing monitoring columns)
- I do not set primary key when creating table despite my primary key recognisation (for join)
    - To check the quality of data and announce about the error
    - OLTP do not allow dupplication for key column(s)


### Areas for Improvement
- Tools:
    - Add dbt for:
        - transforming data
        - data goverance
        - data quality
        - develop in 3 develop evironment (development, staging, production)
    - Airflow 
        - run dbt models  with commands
        - access control
        - dag relationship
        - logs
        - plugins to keep code clean and no repeatation
        - retry mechanism
    - Monitoring job and alert with failed job: 
- Analyse data carefully to provide usefull insight

### Data Insights
- I do not have time to do this part