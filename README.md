The goal of this project is to create a pipeline to get XAU/USD and US30 data from finnhub within a timeframe.
We compare the financial data with any news or sentiments within that timeframe.

The pipeline is designed to collect, process, and store the data in real-time, and make it available for further analysis and reporting.

As a data engineering problem, the project involves the following tasks:

- Use the Finnhub API to pull historical data for XAU/USD and US30 within the specified timeframe, e.g. using the /forex/candle and /index/candle endpoints.

- Store the data in a temporary location, such as a Cloud Storage bucket.
- I'll be Using Prefect to manage and orchestrate the pipeline components.

- Use a sentiment analysis tool or news API to pull relevant news or sentiment data for the same timeframe. Store the data in a temporary location, such as another Cloud Storage bucket.

- Join the data from Finnhub and the sentiment data using a common time range, e.g. by grouping data by timestamp or using a sliding window.

- Cleanse and transform the data as needed to prepare it for loading into BigQuery. This includes renaming columns, casting data types, and dropping or filling missing values.

- Use Cloud Dataproc to run Spark jobs to further transform the data.

- Load the transformed data into a BigQuery table.
- Create dashboards using Google Data Studio, connecting to the BigQuery table.



As you work on each step of the project, you will encounter various data engineering challenges such as data ingestion, data processing, data storage, data modeling, data transformation, data visualization, and pipeline orchestration. The project will allow you to apply your data engineering skills and knowledge to solve real-world data engineering problems.

pip install finnhub-python pandas google-cloud-storage prefect
