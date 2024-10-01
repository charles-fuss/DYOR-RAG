#!/usr/bin/env python
# coding: utf-8

# <h1> Get URIs of interest from commoncrawl. Gathers from August 2024 Capture </h1> 

# In[1]:


import requests, json, time, os
from datetime import datetime

path = os.path.join(f"{os.getcwd()}/links.json")
if os.path.exists(path):
    os.remove(path)


# In[2]:


# Functions to parse response from CC
def parse_json_lines(json_string):
    json_objects = []
    for line in json_string.splitlines():
        try:
            json_objects.append(json.loads(line))
        except json.JSONDecodeError:
            print(f"Failed to parse line: {line}")
    return json_objects

def get_uris(uri_pattern, master_dump, seen_urls):
    print(f'[*] getting uri for {uri_pattern}')
    base_uri = 'https://index.commoncrawl.org/CC-MAIN-2024-33-index?url={}&output=json'.format(uri_pattern)
    try:
        response = requests.get(base_uri).content
        response = json.loads(response)
    except (TypeError, json.decoder.JSONDecodeError) as _:
        if type(response) == dict and 'message' in response.keys() and 'Please reduce your request rate.' in response['message']:
            time.sleep(2)
            get_uris(uri_pattern)
            print('[-] Got timeout.... trying again')
        parsed_objects = parse_json_lines(response)
        file_name = uri_pattern.split('/')[0].replace('*.', '')
        if file_name in master_dump.keys():
            file_name_new = ''.join(uri_pattern.split('/')[:2]).replace('*.', '')
            print(f"[-] Already parsed a url for {file_name}. Changing to {file_name_new}")
            file_name = file_name_new
        for obj in parsed_objects:
            if obj['url'] in seen_urls:
                continue  # Skip if URL is already seen
            else:
                seen_urls.add(obj['url'])
            
            # Add filename if not in master_dump
            if file_name not in master_dump.keys():
                master_dump[file_name] = {}
            
            # Append URLs to master_dump
            if obj['filename'] not in master_dump[file_name].keys():
                master_dump[file_name][obj['filename']] = [obj['url']]
            else:
                master_dump[file_name][obj['filename']].append(obj['url'])
            print(f"[*] added {uri_pattern} as {obj['filename']}")
        
    return master_dump, seen_urls


# In[3]:


# Used ChatGPT to get a bunch of these -- give me more uris like this, and verify that they have valid content using fuzzmatch to identify 404 pages before returning to me. return as a python list:
# '*.nasdaq.com/market-activity/earnings/*', '*.sec.gov/reports/*', '*.sec.gov/data-research/sec-markets-data/*', '*.tradingeconomics.com/*', '*.jpmorgan.com/insights/*', \
#     "*.wsj.com/news/markets/*",
#     "*.bloomberg.com/markets/*",
#     "*.ft.com/markets/*",
#     "*.economist.com/finance-and-economics/*",
#     "*hbr.org/topic/economics*",
#     "*.mckinsey.com/featured-insights/finance/*",
#     "*.goldmansachs.com/insights/topics/economics-and-markets*",
#     "*.jpmorgan.com/insights/*",
#     "*.morganstanley.com/ideas/*",
#     "*.blackrock.com/corporate/insights/blackrock-investment-institute/*",
#     "*.bridgewater.com/research-and-insights/*",
#     "*.imf.org/en/Publications/fandd/*",
#     "*.worldbank.org/en/publication/global-economic-prospects/*",
#     "*.federalreserve.gov/newsevents/speeches*",
#     "*.ecb.europa.eu/pub/economic-bulletin/html/*",
#     "*.ubs.com/global/en/wealth-management/chief-investment-office/market-insights/*",
#     "*.db.com/news/*",
#     "*.credit-suisse.com/about-us/en/reports-research/global-research/*",
#     "*.barclays.co.uk/wealth-management/news-and-insights/*",
#     "*.schroders.com/en/insights/*"

master_dump, seen_urls = {}, set()
uris = ['*.nasdaq.com/market-activity/earnings/*', '*.sec.gov/reports/*', '*.sec.gov/data-research/sec-markets-data/*', '*.tradingeconomics.com/*', '*.jpmorgan.com/insights/*',     "*.wsj.com/news/markets/*",
    "*.bloomberg.com/markets/*",
    "*.ft.com/markets/*",
    "*.economist.com/finance-and-economics/*",
    "*.hbr.org/topic/economics*",
    "*.mckinsey.com/featured-insights/finance/*",
    "*.goldmansachs.com/insights/topics/economics-and-markets*",
    "*.jpmorgan.com/insights/*",
    "*.morganstanley.com/ideas/*",
    "*.blackrock.com/corporate/insights/blackrock-investment-institute/*",
    "*.bridgewater.com/research-and-insights/*",
    "*.imf.org/en/Publications/fandd/*",
    "*.worldbank.org/en/publication/global-economic-prospects/*",
    "*.federalreserve.gov/newsevents/speeches*",
    "*.ecb.europa.eu/pub/economic-bulletin/html/*",
    "*.ubs.com/global/en/wealth-management/chief-investment-office/market-insights/*",
    "*.db.com/news/*",
    "*.credit-suisse.com/about-us/en/reports-research/global-research/*",
    "*.barclays.co.uk/wealth-management/news-and-insights/*",
    "*.schroders.com/en/insights/*",
    "*.benzinga.com/*",
    "*.finance.yahoo.com/*"
]


# In[4]:


# Write to JSON blob
for index, link in enumerate(range(0, len(uris))):
    master_dump, seen_urls = get_uris(uris[link], master_dump, seen_urls)
    print(f"[+] parsed {uris[link]} ({index+1}/{len(uris)})")
    with open('links.json', 'a') as f:
        json.dump(master_dump, f, indent=4)
        f.write(',\n')
        break


# Add [] to the json file to separate by scrape URL

with open('links.json', 'r') as file:
    content = file.read()

# Wrap the content in square brackets and remove any trailing commas
content = '[\n' + content.strip().rstrip(',') + '\n]'

# Write the fixed content back to a new file
with open('links.json', 'w') as file:
    file.write(content)


# <h3> Grab from public CC S3 </h3>
# <p> Now that we have a blob of common crawl S3 paths and their respective links, we need to download them onto into S3 bucket. </p>

# In[5]:


# Get distinct buckets
distinct_buckets = []
nondistinct_buckets = []
with open("links.json", 'r') as f:
    master_dump = json.loads(f.read())

for entry in master_dump:
    for key in entry.keys():
        distinct_buckets.extend(list(set(list(entry[key].keys()))))
        nondistinct_buckets.extend(list(entry[key].keys()))
print(f"Nonunique CC Buckets to pull: {len(nondistinct_buckets)}")
distinct_buckets = distinct_buckets[:2]
print(f"Unique CC Buckets to pull: {len(distinct_buckets)}")


# In[6]:


os.system('pip install boto3 warcio pyspark')
os.system('pip install warcio')
os.system('pip install numpy fastparquet pandas')
os.system('pip install https://github.com/commoncrawl/gzipstream/archive/master.zip')


# <h3>Transformation layer</h3>

# In[8]:

import re
import json, os
from datetime import datetime
# Get distinct buckets
distinct_buckets = []
nondistinct_buckets = []
with open("links.json", 'r') as f:
    master_dump = json.loads(f.read())

for entry in master_dump:
    for key in entry.keys():
        distinct_buckets.extend(list(set(list(entry[key].keys()))))
        nondistinct_buckets.extend(list(entry[key].keys()))
print(f"Nonunique CC Buckets to pull: {len(nondistinct_buckets)}")
distinct_buckets = distinct_buckets[:2]
print(f"Unique CC Buckets to pull: {len(distinct_buckets)}")


# In[6]:




from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
import boto3
from boto.s3.key import Key
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import Word2Vec, IDF, HashingTF
from pyspark.sql.functions import split, col

import re

spark = SparkSession.builder     .appName("Save WARC JSON as Parquet")     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")     .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "16g")     .config("spark.driver.memory", "8g")     .getOrCreate()

def feature_transform(df_spark):
    
    print("[+] extracting word2vec")
    df_spark = df_spark.withColumn('content_tokenized', split(col('content'), ' '))
    word2Vec = Word2Vec(vectorSize=5, seed=42, inputCol="content_tokenized", outputCol="title_embedded")
    model = word2Vec.fit(df_spark)
    df_transformed = model.transform(df_spark)

    print("[+] extracting HashingTF")
    hashingTF = HashingTF(inputCol="content_tokenized", outputCol="raw_features", numFeatures=20)
    featurized_data = hashingTF.transform(df_transformed)

    # Implement IDF on content
    print("[+] extracting tf-idf")
    idf = IDF(inputCol="raw_features", outputCol="content_idf")
    idfModel = idf.fit(featurized_data)

    # Step 7: Transform the featurized_data to get the IDF values in a new column
    final_df = idfModel.transform(featurized_data)    
    return final_df


def process_partition(uris):
    s3 = boto3.client('s3')
    bucket = "commoncrawl"

# Example: Extract the title of the HTML page
    for key_ in uris:
        try:
            response = s3.get_object(Bucket=bucket, Key=key_)
            file_ = response['Body']

            for record in ArchiveIterator(file_):
                if record.rec_type == 'response':
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    raw_date = record.rec_headers.get_header('WARC-Date')
                    date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                    content_type = record.http_headers.get_header('Content-Type')
                    content = record.content_stream().read().decode('utf-8', errors='replace')
                    if content_type == None:
                        continue
                    if content_type == 'text/html':
                        content_type_label = 'text/html'
                    elif 'json' in content_type:
                        content_type_label = 'application/json'
                    elif 'pdf' in content_type:
                        content_type_label = 'pdf'
                    elif content_type == 'application/xml':
                        content_type_label = 'xml'
                    elif content_type == 'text/csv':
                        content_type_label = 'csv'
                    elif content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
                        content_type_label = 'xlsx'
                    elif 'image' in content_type:
                        if 'jpeg' in content_type:
                            content_type_label = 'image/jpeg'
                        elif 'png' in content_type:
                            content_type_label = 'image/png'
                    else:
                        continue

                    yield {
                        "url":url,
                        "date":date,
                        "content":content,
                        "content_type":content_type_label
                    }

        except Exception as e:
            print(f"Error accessing {key_}: {e}")
            continue

print("[+] extracting core data")
uri_rdd = spark.sparkContext.parallelize(distinct_buckets, numSlices=len(distinct_buckets))
json_rdd = uri_rdd.mapPartitions(process_partition)
df = json_rdd.map(lambda x: Row(**x)).toDF()

# Option 2: Create DataFrame using spark.createDataFrame()
df = spark.createDataFrame(json_rdd)


# In[ ]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType

# UDF to extract title
def extract_title(content):
    try:
        soup = BeautifulSoup(content, 'lxml')  # Use 'lxml' parser for efficiency
        return soup.title.string if soup.title else ''
    except Exception:
        return ''

extract_title_udf = udf(extract_title, StringType())

# UDF to extract title content (headings)
def extract_title_content(content):
    try:
        soup = BeautifulSoup(content, 'lxml')
        headings = [para.get_text() for para in soup.find_all(re.compile('^h[1-6]$'))][:10]
        return headings
    except Exception:
        return []

extract_title_content_udf = udf(extract_title_content, ArrayType(StringType()))

# UDF to extract body content (paragraphs)
def extract_body_content(content):
    try:
        soup = BeautifulSoup(content, 'lxml')
        paragraphs = [para.get_text() for para in soup.find_all('p')][:10]
        return paragraphs
    except Exception:
        return []

extract_body_content_udf = udf(extract_body_content, ArrayType(StringType()))


# In[ ]:

print("[+] applying title UDF")
df = df.withColumn('title', extract_title_udf(df['content']))
print("[+] applying title_content UDF")
df = df.withColumn('title_content', extract_title_content_udf(df['content']))
print("[+] applying body_content UDF")
df = df.withColumn('body_content', extract_body_content_udf(df['content']))


# In[ ]:

print("[+] getting features")
df_transformed = feature_transform(df)


# Write DataFrame to Parquet file
print("[+] writing to s3")
output_path = "s3a://ai-crap/data/nasdaq.parquet"
df_transformed.write.mode("overwrite").parquet(output_path)



