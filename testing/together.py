# import spacy
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec
from pyspark.ml.feature import HashingTF, IDF


# def tokenize(query):
#     doc = nlp(query)
#     return doc

def vectorize(wordsData):
    # CountVectorizer for term frequencies
    countVectorizer = CountVectorizer(inputCol="words", outputCol="rawFeatures", vocabSize=10000)
    cvModel = countVectorizer.fit(wordsData)
    featurizedData = cvModel.transform(wordsData)

    # Apply IDF
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    



if __name__ == '__main__':
    spark = SparkSession.builder     .appName("Save WARC JSON as Parquet")     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")     .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "16g")     .config("spark.driver.memory", "8g")     .getOrCreate()
    file_path = r"part-00000-77745408-e261-4890-8d8b-0832eb0a7130-c000.snappy.parquet"
    
    doc = pd.read_parquet(file_path)
    doc['content']= doc['content'].map(lambda x: x.decode('utf-8', errors='ignore') if isinstance(x, bytes) else x)
    doc['content_tokenized'] = doc['content'].map(lambda x: x.split() if isinstance(x, str) else [])
    df_spark = spark.createDataFrame(doc)
    
    word2Vec = Word2Vec(vectorSize=5, seed=42, inputCol="content_tokenized", outputCol="content_embedded")
    df_embeddings = word2Vec.fit(df_spark).transform(df_spark)

    # Step 5: Apply HashingTF to create rawFeatures from content_tokenized
    # At this point, make sure you're applying the transformation on df_embeddings
    hashingTF = HashingTF(inputCol="content_tokenized", outputCol="rawFeatures", numFeatures=20)
    featurized_data = hashingTF.transform(df_embeddings)

    # Step 6: Apply IDF on rawFeatures to get the final TF-IDF values
    idf = IDF(inputCol="raw_features", outputCol="content_idf")
    idfModel = idf.fit(featurized_data)

    # Step 7: Transform the featurized_data to get the IDF values in a new column
    final_df = idfModel.transform(featurized_data)

    # Step 8: Drop extraneous columns
    final_df = final_df.drop('content_tokenized').drop('raw_features')

    # Step 9: Write the final DataFrame with embeddings and TF-IDF to S3
    output_path = "s3a://ai-crap/data/embeddings.parquet"
    final_df.write.mode("overwrite").parquet(output_path)