import requests
import json
import math

def append_to_json_lines(filename, new_data):
    # Open the file in append mode ('a')
    with open(filename, 'a') as f:
        # Convert the dictionary to a JSON string
        json_string = json.dumps(new_data)
        
        # Write the JSON string followed by a newline character
        f.write(json_string + '\n')

filename = "airports.jsonl"

for offset in range(1, math.ceil(6702 / 100)):
  params = {
    'access_key': '055c63e139fffdaf071121a88fca3098',
    'offset': offset * 100
  }

  api_result = requests.get('https://api.aviationstack.com/v1/airports', params)

  api_response = api_result.json()

  # Use a 'with' statement to open the file and ensure it closes automatically
  append_to_json_lines(filename, api_response)