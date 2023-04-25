import requests
import csv
results = []

for round in range(1, 23):
    for year in range(2005, 2020):
        response = requests.get(f'http://ergast.com/api/f1/{year}/{round}/results.json')
        data = response.json()
        results.append(data['MRData']['RaceTable']['Races'][0])

print(results)



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
