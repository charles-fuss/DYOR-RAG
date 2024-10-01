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

