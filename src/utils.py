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

def get_data(api_url, start_offset, end_offset, access_key, filename):
  for offset in range(start_offset, end_offset):
  # for offset in range(0, 1):
    params = {
      'access_key': access_key,
      'offset': offset * 100
    }

    api_result = requests.get(api_url, params)
    api_response = api_result.json()

    # Use a 'with' statement to open the file and ensure it closes automatically
    append_to_json_lines(filename, api_response)
    print(f"Appended data for offset {offset} to {filename}")

def load_json(filename):
    with open(filename, 'r') as f:
      data = json.load(f)
    return data

def save_data(input_filename, output_filename):
  data = load_json(input_filename)
  items_list = []
  for obj in data:
    items_list.extend(obj['data'])
  with open(output_filename, 'w') as f:
    json.dump(items_list, f, indent=2)
  print(f"Processed {len(items_list)} items and saved to '{output_filename}'")

def process_data(input_filename, output_filename):
  data = load_json(input_filename)
  processed_data = []
  for item in data:
    for key, value in item.items():
      if isinstance(value, str):
        value = value.strip()
      item[key] = value
    processed_data.append(item)
  with open(output_filename, 'w') as f:
    json.dump(processed_data, f, indent=2)
  print(f"Processed {input_filename} data saved to '{output_filename}'")
