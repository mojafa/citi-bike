import requests
import csv

results = []

for year in range(2018,2021):
    for round in range(1, 23):
        response = requests.get(f'http://ergast.com/api/f1/{year}/{round}/results.json')
        data = response.json()
        results.append(data['MRData']['RaceTable']['Races'])
print(results)


with open('race_results.csv', mode='w', newline='') as file:

    writer = csv.writer(file)
    writer.writerow(['round', 'raceName', 'date', 'winner', 'team', 'fastestLap', 'averageSpeed'])
    for race in results:
        round_num = race['round']
        race_name = race['raceName']
        date = race['date']
        fastestLap = race['fastestLap']
        averageSpeed = race['averageSpeed']
        winner = race['Results'][0]['Driver']['givenName'] + ' ' + race['Results'][0]['Driver']['familyName']
        team = race['Results'][0]['Constructor']['name']
        writer.writerow([round_num, race_name, date, winner, team])
