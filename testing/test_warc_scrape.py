#!/usr/bin/env python
# coding: utf-8

# In[2]:



import boto3, json
from datetime import datetime
from warcio.archiveiterator import ArchiveIterator

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


# In[3]:




def process_partition(uris):
    s3 = boto3.client('s3')
    bucket = "commoncrawl"

    for key_ in uris:
            try:
                key_ = key_.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz') 
                response = s3.get_object(Bucket=bucket, Key=key_)
                file_ = response['Body']

                for record in ArchiveIterator(file_):
                    breakpoint()
                    print('response')
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    raw_date = record.rec_headers.get_header('WARC-Date')
                    date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                    content_type = record.http_headers.get_header('Content-Type')
                    try:
                        content = record.content_stream().read().decode('utf-8')
                    except:
                        content = record.content_stream().read().decode('utf-8')
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

                    return {
                        "url":url,
                        "date":date,
                        "content":content,
                        "content_type":content_type_label
                    }

            except Exception as e:
                print(f"Error accessing {key_}: {e}")
                continue


# In[4]:


process_partition(distinct_buckets)

