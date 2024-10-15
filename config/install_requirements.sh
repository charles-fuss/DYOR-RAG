#!/bin/bash

# Install necessary Python packages using pip
pip install boto3 warcio pyspark pysparknlp
pip install warcio
pip install numpy fastparquet pandas
pip install https://github.com/commoncrawl/gzipstream/archive/master.zip

# Repeating installations as in the original script (not necessary, but included for accuracy)
pip install warcio
pip install boto3 warcio pyspark
pip install warcio
pip install https://github.com/commoncrawl/gzipstream/archive/master.zip

# Install specific versions of packages
pip install spark-nlp==5.5.0