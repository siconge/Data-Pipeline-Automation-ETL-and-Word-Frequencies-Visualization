import json, os

# Copy 'words.json' from Airflow container's file system to the host machine
file_path = os.path.join(os.getcwd(), 'words.json')
cmd = f'docker cp airflow-docker-airflow-worker-1:/opt/airflow/words.json {file_path}'
result = os.system(cmd)
if result == 0:
    print('words.json created')

# Read data from 'words.json' and load it into a Python dictionary
with open('words.json', 'r') as f:
    data = json.load(f)

# Restructure the data into a list of dictionaries for JavaScript usage
freq_dict = {'words':[{'Name':key, 'Count':value} for key, value in data.items()]}

# Convert the restructured data into a JSON string and write it to 'words.js'
js_content = 'freq_dict = ' + json.dumps(freq_dict, indent=4) + ';'
with open('words.js', 'w') as f:
    f.write(js_content)
print('words.js created')