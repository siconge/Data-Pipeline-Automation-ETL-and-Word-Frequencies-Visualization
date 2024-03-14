# DAG object and operators
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Task facilitating functions
import urllib.request
import time
import glob, os
import json

# Helper function for requesting and parsing data from an data source URL
def pull(url):
    with urllib.request.urlopen(url) as url:
        return url.read().decode('utf-8')
        
# Helper function for storing data in a HTML file
def store_html(data, file):
    with open(file, 'w') as f:
        f.write(data)
    print('wrote file: ' + file)

# Helper function for storing data in a JSON file
def store_json(data, file):
    with open(file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print('wrote file: ' + file)

# TASK 1 - Pull course catalog pages
def catalog():
    
    # Create a list that contains the working URLs as strings
    dag_dir = os.path.dirname(os.path.abspath(__file__)) # __file__ represents the path to the currently executed script
    file_path = os.path.join(dag_dir, 'mit_courses_urls.txt')
    with open(file_path, 'r') as f:
        urls = f.read().split('\n')
    
    # Store parsed data into the corresponding file named with a URL suffix (e.g. m1a.html)
    for url in urls:
        data = pull(url)
        index = url.rfind('/') + 1
        file = url[index:]
        store_html(data, file)
        print('pulled: ' + file)
        print('--- waiting ---')

        # Allow for sleep time between consecutive requests to avoid triggering security measures on target server
        time.sleep(15)

# TASK 2 - Concatenate all files into a combo file
def combine():
    with open('combo.txt', 'w') as outfile:
        for file in glob.glob('*.html'):
            with open(file) as infile:
                outfile.write(infile.read())

# TASK 3 - Store course titles scraped from HTML in a JSON file
def titles():    
    
    # Preprocess HTML text data in 'combo.txt' generated by TASK 2 by handling line breaks and carriage returns
    with open('combo.txt', 'r') as f:
        html = f.read().replace('\n', ' ').replace('\r', '')
    
    # Create a HTML parser using BeautifulSoup Library
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser') 
    results = soup.find_all('h3')

    # Store the text content of the specified tag in a list
    titles = []
    for item in results:
        titles.append(item.text)
    
    store_json(titles, 'titles.json')

# TASK 4 - Perform data cleaning for stored course titles
def clean():
    
    # Set up stopwords using NLTK Library
    import nltk
    from nltk.corpus import stopwords
    nltk.download('stopwords')
    nltk.download('punkt')
    stop_words = set(stopwords.words('english'))
    
    with open('titles.json', 'r') as f:
        titles = json.load(f)
    
    # Tokenize and filter titles by removing punctuation, numbers, and stopwords, then update the title list
    for idx, title in enumerate(titles):
        tokens = nltk.word_tokenize(title)
        filtered_tokens = [word for word in tokens if word.isalpha() and word.lower() not in stop_words and len(word)>1]
        titles[idx] = ' '.join(filtered_tokens)
    
    store_json(titles, 'titles_clean.json')

# TASK 5 - Count word frequency of cleaned course titles
def count_words():
    with open('titles_clean.json', 'r') as f:
        titles = json.load(f)
    
    # Extract words and flatten
    words = []
    for title in titles:
        words.extend(title.split())
    
    # Count word frequency
    from collections import Counter
    counts = Counter(words)
    
    store_json(counts, 'words.json')

# Instantiate a DAG object and design an Airflow pipeline
with DAG(
    'mit_courses_etl_pipeline',
    start_date=days_ago(1),
    schedule_interval='@daily',catchup=False,
) as dag:
    
    # Create tasks by instantiating operators
    t0 = BashOperator(task_id='task_zero', bash_command='pip install beautifulsoup4 && pip install nltk', retries=2)
    t1 = PythonOperator(task_id='task_one', depends_on_past=False, python_callable=catalog)
    t2 = PythonOperator(task_id='task_two', depends_on_past=False, python_callable=combine)
    t3 = PythonOperator(task_id='task_three', depends_on_past=False, python_callable=titles)
    t4 = PythonOperator(task_id='task_four', depends_on_past=False, python_callable=clean)
    t5 = PythonOperator(task_id='task_five', depends_on_past=False, python_callable=count_words)

    # Chain multiple dependencies between tasks
    t0 >> t1 >> t2 >> t3 >> t4 >> t5