import sys
import datetime
from pyspark import SparkContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
import nltk

nltk.download('stopwords')

sc = SparkContext(appName="etl2")

s3_base_dataset_path = 's3://sandonas-bigdata-datasets/COVID19-twitter/'
delimiter='/'

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
year = yesterday.year
month = '{:02d}'.format(yesterday.month)
day = '{:02d}'.format(yesterday.day)

s3_path = s3_base_dataset_path + str(year) + delimiter + str(month) + delimiter + str(day) + delimiter + '*'

schema = StructType([
  StructField("timestamp", LongType(), True),
  StructField("text", StringType(), True),
  StructField("country_code", StringType(), True),
  StructField("user_followers_count", IntegerType(), True),
  StructField("tweet_id", LongType(), False),
  StructField("hashtags", StringType(), True)])

df = spark.read.option("delimiter", ";").schema(schema).csv(s3_path)

# fix group by country_code ->

numberOfCovidTweetsRetrieved = df.count()

words_split_and_explode = f.explode(f.split(f.col('text'), ' '))

# sudo pip install nltk -> remove stop words

top10words = df.withColumn('top10words', words_split_and_explode) \
    .filter((f.col("top10words") != '')) \
    .groupBy('top10words') \
    .count() \
    .sort('count', ascending=False) \
    .show()

hashtags_without_brackets = df.withColumn("hashtags",f.expr("substring(hashtags, 2, length(hashtags)-2)"))
hashtags_split_and_explode = hashtags_without_brackets.withColumn("hashtags", f.explode(f.split(f.col('hashtags'), ", ")))
hashtags_remove_apexes = hashtags_split_and_explode.withColumn("hashtags", f.expr("substring(hashtags, 2, length(hashtags)-2)"))

top10pounds = hashtags_remove_apexes.withColumn('top10pounds', f.col("hashtags")) \
    .filter((f.col("top10pounds") != 'None') & (f.col("top10pounds") != '')) \
    .groupBy('top10pounds') \
    .count() \
    .sort('count', ascending=False) \
    .show()

# input:  timestamp, text, country_code, user_followers_count, tweet_id, [hashtags]

# output: country_code, day, numberOfCovidTweetsRetrieved, top 10 words, top 10 hashtags

sc.stop()