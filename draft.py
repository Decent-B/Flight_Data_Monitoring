import requests

params = {
  'access_key': '055c63e139fffdaf071121a88fca3098',
  'iataCode': 'JFK',
  'type': 'departure'
}

api_result = requests.get('https://api.aviationstack.com/v1/timetable', params)

api_response = api_result.json()

print(api_response['data'])