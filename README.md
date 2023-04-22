The goal of the finnhub-streaming-data-pipeline project is to build a scalable and fault-tolerant streaming data pipeline for processing real-time financial data from the Finnhub API.

 The pipeline is designed to collect, process, and store financial data in real-time, and make it available for further analysis and reporting.

As a data engineering problem, the project involves the following tasks:

Collecting real-time financial data from the Finnhub API and sending it to a Kafka topic.
Processing the data in real-time using Apache Spark.
Storing the processed data in a scalable and fault-tolerant data storage system such as Google Cloud Storage and BigQuery.
Using dbt to model and transform the data.
Using Looker to visualize and report on the processed data.
Using Prefect to manage and orchestrate the pipeline components.
The project involves working with several data engineering technologies such as Kafka, Spark, GCS, BQ, dbt, Looker, and Prefect to build a complete end-to-end streaming data pipeline.

As you work on each step of the project, you will encounter various data engineering challenges such as data ingestion, data processing, data storage, data modeling, data transformation, data visualization, and pipeline orchestration. The project will allow you to apply your data engineering skills and knowledge to solve real-world data engineering problems.


# Set up a scheduled task to trigger the pipeline, e.g. using a cron job or Cloud Scheduler.
# Use the Finnhub API to pull historical data for XAU/USD and US30 within the specified timeframe, e.g. using the /forex/candle and /index/candle endpoints.
# Store the data in a temporary location, such as a Cloud Storage bucket.
# Use a sentiment analysis tool or news API to pull relevant news or sentiment data for the same timeframe. Store the data in a temporary location, such as another Cloud Storage bucket.
# Use Apache Beam or another ETL tool to join the data from Finnhub and the sentiment data using a common time range, e.g. by grouping data by timestamp or using a sliding window. You can use Python's Pandas library to join the data using a common time range as well.
# Cleanse and transform the data as needed to prepare it for loading into BigQuery. This may include renaming columns, casting data types, and dropping or filling missing values.
# Use Cloud Dataproc to run Spark jobs to further analyze or transform the data, e.g. using Spark SQL or MLlib.
# Load the transformed data into a BigQuery table.
# Create dashboards using a data visualization tool such as Google Data Studio, connecting to the BigQuery table.
