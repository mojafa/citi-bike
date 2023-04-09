import requests

symbol = 'AAPL'  # replace with your desired stock symbol
api_key = 'cgm7u01r01qlbmq7l5g0cgm7u01r01qlbmq7l5gg'  # replace with your Finnhub API key

url = f'https://finnhub.io/api/v1/quote?symbol={symbol}&token={api_key}'
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data['c'])  # print the current stock price
else:
    print(f'Request failed with status code: {response.status_code}')
