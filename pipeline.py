#Import libraries
import os
import pandas as pd
import requests
import time
import datetime
import subprocess
from prefect_gcp import GcpCredentials, GcsBucket
from prefect import task, Flow
from prefect.task_runners import SequentialTaskRunner
from prefect.deployments import Deployment



@task
def run_pipeline():
    # Get the current date and time
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Trigger the pipeline
    trigger_pipeline(now)

# Schedule the flow to run at a specific time every day
with Flow('daily-pipeline') as flow:
    run_pipeline()

flow.schedule = Schedule(clocks=[IntervalClock(start_date=datetime.datetime(2022, 1, 1), interval=datetime.timedelta(days=1))])

import requests
import json

start_time = '2022-01-01'
end_time = '2022-03-31'
resolution = 'D'

response = requests.get(f'https://finnhub.io/api/v1/forex/candle?symbol=XAUUSD&resolution={resolution}&from={start_time}&to={end_time}&token=YOUR_API_KEY')
response = requests.get(f'https://finnhub.io/api/v1/forex/candle?symbol=^DJI&resolution={resolution}&from={start_time}&to={end_time}&token=YOUR_API_KEY')

data = json.loads(response.text)

# Set up a scheduled task to trigger the pipeline, e.g. using a cron job or Cloud Scheduler.
# Use the Finnhub API to pull historical data for XAU/USD and US30 within the specified timeframe, e.g. using the /forex/candle and /index/candle endpoints.
# Store the data in a temporary location, such as a Cloud Storage bucket.
# Use a sentiment analysis tool or news API to pull relevant news or sentiment data for the same timeframe. Store the data in a temporary location, such as another Cloud Storage bucket.
# Use Apache Beam or another ETL tool to join the data from Finnhub and the sentiment data using a common time range, e.g. by grouping data by timestamp or using a sliding window. You can use Python's Pandas library to join the data using a common time range as well.
# Cleanse and transform the data as needed to prepare it for loading into BigQuery. This may include renaming columns, casting data types, and dropping or filling missing values.
# Use Cloud Dataproc to run Spark jobs to further analyze or transform the data, e.g. using Spark SQL or MLlib.
# Load the transformed data into a BigQuery table.
# Create dashboards using a data visualization tool such as Google Data Studio, connecting to the BigQuery table.

@task(name="pull_stock_data", log_prints=True)
def pull_financial_data(symbols, finnhub_key, from_date, to_date):
    #Dictionary to store raw data
    exchange_data = {}
    start_time = '2022-01-01'
    end_time = '2022-03-31'

    #Counter to keep track of API calls
    counter = 1

    for symbol in symbols.keys():
        #Use try to avoid unanticipated API errors
        try:
            if counter%6 != 0:
                #Update URL
                url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=' + symbol + '&apikey=' + finnhub_key + '&outputsize=full'
                response = requests.get(url)
                stock_data[symbol] = response.json()['Time Series (Daily)']
                counter += 1

                    US30
                # https://finnhub.io/api/v1/index/constituents?symbol=^DJI&token=YOUR_API_KEY
                    XAUUSD
                # https://finnhub.io/api/v1/forex/rates?base=XAU&token=YOUR_API_KEY


            else:
                #Pause after 5 calls
                print('Reached max 5 API calls per minute. Pausing for 60s...')
                time.sleep(62)
                counter += 1
        except:
            #Print exceptions for respective stocks/symbols
            print('SSLError : Unable to pull data for ' + symbol)
            counter += 1


    #Prepare dataframe
    df_stock = []
    df_day = []
    df_value = []

    for stock in stock_data.keys():
        for day in stock_data[stock].keys():
            df_stock.append(stock)
            df_day.append(day)
            #We will capture adjusted closing value
            df_value.append(stock_data[stock][day]['5. adjusted close'])

    #Create dataframe
    stock_df = pd.DataFrame({'Stock_Name' : [symbols[i] for i in df_stock], 'Stock': df_stock, 'Day': df_day, 'Value': df_value})
    #Apply filters to only capture data as per from and to dates that were passed
    stock_df = stock_df[(stock_df['Day'] >= from_date) & (stock_df['Day'] < to_date)]
    #Save data locally for reference
    stock_df.to_csv(os.path.join(os.getcwd(),'stocks_df_' + from_date + '_' + to_date + '.csv'),index=False)
    #Return dataframe
    return stock_df

# B - Pull time series sentiments data in batches - 5 Days at a time using Alpa Vantage API
#Arguments for flow - Symbols dictionary, Alpha Vantage key, From date and To date to set period
#Returns stock sentiment dataframe
@task(name="pull_time_series_stock_sentiment_data", log_prints=True)
def pull_time_series_stock_sentiment_data(symbols, finnhub_key, from_date, to_date):
    #Iterate through each week to create a list of mondays - Alpha vantage API sentiment data has a limit of 200 responses at a time. So we will only pull 5 days data at a time. From each monday to friday.
    temp_day = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()
    mondays = [temp_day]
    while temp_day + datetime.timedelta(days=7) < datetime.datetime.strptime(to_date, "%Y-%m-%d").date():
        temp_day = temp_day + datetime.timedelta(days=7)
        mondays.append(temp_day)


    #Dictionary to store raw data
    stock_sentiment_data = {}

    #5 calls can be made to aplha vantage per minute. So after every 5 calls we will pause for 1 min with 2 secs buffer
    #Counter to track calls
    counter = 1
    #Iterate through each symbol
    for symbol in symbols.keys():
        #Iterate trhough each monday
        for monday in mondays:
            #Use try to avoid unanticipated API errors
            try:
                if counter%6 != 0:
                    from_day = str(monday).replace('-','') + 'T0000'
                    to_day = str(monday + datetime.timedelta(days=5)).replace('-','') + 'T0000'
                    #Update URL
                    url = 'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=' + symbol + '&apikey=' + finnhub_key + '&topics=technology&time_from=' + from_day + '&time_to=' + to_day + '&limit=200'
                    stock_sentiment_data[symbol + '_' + str(monday)] = requests.get(url).json()
                    counter += 1
                else:
                    print('Reached max 5 API calls per minute. Pausing for 60s...')
                    time.sleep(62)
                    counter += 1
            except:
                #Print exceptions for respective stocks/symbols & dates
                print('SSLError : Unable to pull data for ' + symbol + ' ' + from_day + ' ' + to_day)
                counter += 1

    #Prepare dataframe
    df_stock = []
    df_time = []
    df_day = []
    df_url = []
    df_title = []
    df_sentiment = []

    for key in stock_sentiment_data.keys():
        try:
            for item in stock_sentiment_data[key]['feed']:
                df_stock.append(key.split('_')[0])
                df_time.append(item['time_published'])
                df_day.append(item['time_published'].split('T')[0][:4] + '-' + item['time_published'].split('T')[0][4:6] + '-' + item['time_published'].split('T')[0][6:8])
                df_url.append(item['url'])
                df_title.append(item['title'])
                df_sentiment.append(item['overall_sentiment_score'])
        except:
            #Print exceptions incase no data is returned
            print('No sentiment data for ' + key)


    #Create dataframe
    stock_sentiment_df = pd.DataFrame({'Stock_Name' : [symbols[i] for i in df_stock], 'Stock': df_stock, 'Time' : df_time, 'Day': df_day, 'URL' : df_url, 'Title': df_title, 'Sentiment' : df_sentiment, 'Record_Count' : [1]*len(df_sentiment) })
    #Save data locally for reference
    stock_sentiment_df.to_csv(os.path.join(os.getcwd(),'stocks_sentiment_df_' + from_date + '_' + to_date + '.csv'),index=False)
    #Return dataframe
    return stock_sentiment_df



#Part 2 - Upload data & pyspark script to GCS
#Arguments for flow - GCP key path, GCS bucket name, From date and To date for unique naming
@task(name="upload_data_to_gcs", log_prints=True)
def upload_to_gcs(gcp_key_path, gcs_bucket_name, from_date, to_date):
    #Save GCP credentials for Prefect
    try:
        GcpCredentials(service_account_file=gcp_key_path).save("gcp-access-block")
        print('GCP credentials block created for Prefect')
    except:
        print('GCP credentials block already created')

    #Load GCP credentials block
    gcp_credentials = GcpCredentials.load("gcp-access-block")
    gcs_bucket = GcsBucket(bucket=gcs_bucket_name,gcp_credentials=gcp_credentials)
    #Upload dataframes and spark job script
    gcs_bucket.upload_from_path(os.path.join(os.getcwd(),'stocks_df_' + from_date + '_' + to_date + '.csv'))
    gcs_bucket.upload_from_path(os.path.join(os.getcwd(),'stocks_sentiment_df_' + from_date + '_' + to_date + '.csv'))
    gcs_bucket.upload_from_path(os.path.join(os.getcwd(),'spark_job.py'))
    return


#Part 3 - Run Spark job
#Arguments for flow - Spark cluster name, Spark job region and GCS bucket name
@task(name="run_spark_job", log_prints=True)
def submit_spark_job(spark_cluster_name, spark_job_region, gcs_bucket_name, bq_dataset_name, bq_table_name, from_date, to_date, spark_job_file):
    #Write gcloud command to run in subprocess
    command_to_run = "gcloud dataproc jobs submit pyspark --cluster=" + spark_cluster_name + " --region=" + spark_job_region + " --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar" + " gs://" + gcs_bucket_name + "/" + spark_job_file + " -- " +  "--gcs_bucket_name=" + gcs_bucket_name + " --bq_dataset_name=" + bq_dataset_name + " --bq_table_name=" + bq_table_name + " --from_date=" + from_date + " --to_date=" + to_date
    #Run command in subprocess
    process = subprocess.run(command_to_run, shell=True, capture_output=True)
    if process.returncode != 0:
        print('Job failed. Please check logs in Dataproc cluster.')
        print('Error message : ' + str(process.stderr))
    else:
        print('Job submitted successfully with following message : ' + str(process.stdout))
    return


@flow(name="main_flow", log_prints=True, task_runner=SequentialTaskRunner())
def main_flow(gcp_key_path, finnhub_key, from_date, to_date, gcs_bucket_name, bq_dataset_name, bq_table_name, spark_cluster_name, spark_job_region, spark_job_file):
    #Set a timer to track time to complete
    start = time.time()
    #Dictionary of top 5 technology stocks with their symbol/ticker and names. These have been identified by market cap and their sentiment data available via Alphavantage API. Market cap & tickets at -> https://www.nasdaq.com/market-activity/stocks/screener
    symbols = {'XAUUSD' : 'Gold', 'DJI' : 'US30'}
    #Part 1 - Fetch data - Call function
    pull_stock_data(symbols, finnhub_key, from_date, to_date)
    #Pause between data fetches to avoid limit breach
    print('Pausing 60s between data stock & sentiment data fetch.')
    time.sleep(60)
    pull_time_series_stock_sentiment_data(symbols, finnhub_key, from_date, to_date)
    #Part 2 - Upload data & pyspark script to GCS - Call function
    upload_to_gcs(gcp_key_path, gcs_bucket_name, from_date, to_date)
    #Part 3 - Run Spark job - Call function
    #Pause to let data get loaded into GCS
    print('Pausing 60s before submitting spark job.')
    time.sleep(60)
    submit_spark_job(spark_cluster_name, spark_job_region, gcs_bucket_name, bq_dataset_name, bq_table_name, from_date, to_date, spark_job_file)
    #Print completion message
    completion_time = str(datetime.timedelta(seconds = time.time() - start)).split('.')[0].split(':')
    print('Job complete : Took ' + completion_time[1] + ' mins & ' + completion_time[2] + ' seconds')



#Code to build deployment of main flow
deployment = Deployment.build_from_flow(
    flow=main_flow,
    name="Data Pipeline Main Flow - DE ZC 2023",
    parameters=
                {"gcp_key_path" : "/app/codes/gcp_key.json",
                "finnhub_key" : "finnhub_key",
                "from_date" : "2023-03-27",
                "to_date": "2023-04-03",
                "gcs_bucket_name": "your_gcs_bucket_name",
                "bq_dataset_name" : "your_bq_dataset_name",
                "bq_table_name" : "your_bq_table_name",
                "spark_cluster_name" : "your-spark-cluster-name",
                "spark_job_region" : "your_region",
                "spark_job_file" :"spark_job.py"}
)

#Call deployment when the script is run
if __name__ == "__main__":
    deployment.apply()
