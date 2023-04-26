import requests
import pandas as pd

# Set API key and base URL
api_key = 'MZEBJG8H3L5VEMBY'
base_url = 'https://www.alphavantage.co/query'

# Set query parameters for news data
news_params = {
    'function': 'NEWS_SENTIMENT',
    'tickers':'TSLA', # tickers
    'topics': 'earnings',  # topics
    'time_from': '20230301T0000',
    'time_to': '20230331T0000',
    'sort':'RELEVANCE',
    'limit':'200',
    'apikey': api_key
}

# Make API request for news data and parse response as JSON
try:
    news_response = requests.get(base_url, params=news_params)
    news_data = news_response.json()
    print(news_data)

except requests.exceptions.RequestException as e:
    print('Error:', e)
    exit(1)

# Convert news JSON response to pandas DataFrame
news_df = pd.json_normalize(news_data, record_path='data')
news_df = news_df[['time_published', 'title', 'ticker_sentiment_label']]

# Filter news DataFrame to March 2023
news_df['time_published'] = pd.to_datetime(news_df['datetime'])
news_df = news_df[news_df['time_published'].dt.year == 2023]
news_df = news_df[news_df['time_published'].dt.month == 3]

# Write news DataFrame to CSV file
news_df.to_csv('tesla_news_sentiment_march_2023.csv', index=False)
