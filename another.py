from prefect import task, Flow
from google.cloud import storage
import csv

# Define the task to get the race results
@task
def get_race_results():
    for round in range(1, 23):
        results = []
        for year in [2018, 2019, 2020, 2021]:
        response = requests.get(f'http://ergast.com/api/f1/{year}/{round}/results.json')
        data = response.json()
        results.append(data['MRData']['RaceTable']['Races'][0])
        print(results)

# Define the task to write the race results to a CSV file
@task
def write_to_csv(results):
    with open('race_results.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['round', 'raceName', 'date', 'winner', 'team'])
        for race in results:
            round_num = race['round']
            race_name = race['raceName']
            date = race['date']
            winner = race['Results'][0]['Driver']['givenName'] + ' ' + race['Results'][0]['Driver']['familyName']
            team = race['Results'][0]['Constructor']['name']
            writer.writerow([round_num, race_name, date, winner, team])

# Define the task to upload the CSV file to GCS
@task
def upload_to_gcs():
    # Set the name of the GCS bucket and the name of the file in the bucket
    bucket_name = 'finnhub-gcs'
    blob_name = 'race_results.csv'

    # Create a new GCS client and get the bucket
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Upload the CSV file to the GCS bucket
    blob = bucket.blob(blob_name)
    blob.upload_from_filename('race_results.csv')

# Define the Prefect flow
    with Flow('F1 Race Results Pipeline') as flow:
        results = get_race_results()
        write_to_csv(results)
        upload_to_gcs()

    # Run the Prefect flow
    flow.run()

if __name__ == "__main__":
    etl_web_to_gcs()
