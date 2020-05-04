import sys
import datetime
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as f
import re

sc = SparkContext(appName="etl2")

s3_base_dataset_path = 's3://sandonas-bigdata-datasets/COVID19-twitter/'
delimiter='/'

# Creating the S3 path to read the data of the day
# TODO year, month, day can be an input of the job
# so that is not hardcoded and we can run initial bulk load or reruns
today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
year = yesterday.year
month = '{:02d}'.format(yesterday.month)
day = '{:02d}'.format(yesterday.day)

s3_path = s3_base_dataset_path + str(year) + delimiter + str(month) + delimiter + str(day) + delimiter + '*'

# Remembering input and output columns for this job
# input:  timestamp, text, country_code, user_followers_count, tweet_id, [hashtags]
# output: country_code, day, numberOfCovidTweetsRetrieved, top10words, top10hashtags

schema = StructType([
  StructField("timestamp", LongType(), True),
  StructField("text", StringType(), True),
  StructField("country_code", StringType(), True),
  StructField("user_followers_count", IntegerType(), True),
  StructField("tweet_id", LongType(), False),
  StructField("hashtags", ArrayType(StringType(), True))])

# ==================== Data quality ====================

# Removing semicolons in field 'text', since it is the field separator

def remove_semicolon_in_text(line):
  # TODO check malformed rows
  cleaned_line = "0;error;RR;0;0;error"
  encoded_line = line.encode('utf-8')
  pattern = "([0-9]*;)('.*');([A-Z]{2};)([0-9]*;)([0-9]*;)(\['.*'\])"
  regex_result = re.match(pattern, encoded_line)
  try:
    text = regex_result.groups()[1]
    text_without_semicolon = text.replace(';', '')
    cleaned_line = re.sub("([0-9]*;)('.*');([A-Z]{2};)([0-9]*;)([0-9]*;)(\['.*'\])", r"\1"+text_without_semicolon+r";\3\4\5\6", encoded_line)
    cleaned_line = cleaned_line.decode('utf-8')
  except AttributeError:
    print("Non-matching line: "+encoded_line)
  return cleaned_line

def cast_line(line):
  casted_line = line
  try:
    casted_line = [long(line[0]),line[1],line[2],int(line[3]),long(line[4]),line[5][1:-1].split(', ')]
  except ValueError:
    print("Line cast failed: "+line)
  return casted_line

# Creating first rdd reading the Stefano's bucket
rdd_without_semicolon = sc.textFile(s3_path).map(lambda line: remove_semicolon_in_text(line)).map(lambda line: line.split(";")).map(lambda line: cast_line(line))

# creating the first dataframe from the rdd, adding the 'day' column
df = spark.createDataFrame(rdd_without_semicolon, schema).withColumn('day', f.lit(day))


# ==================== Top 10 Words ====================

# group by country,day and concatenating the 'text' field
df_groupByCountry_text = df \
  .groupBy('country_code','day') \
  .agg( \
    f.concat_ws(" ", f.collect_list('text')).alias("text_concat"), \
    f.count(f.lit(1)).alias("numberOfCovidTweetsRetrieved")) \
  .sort('numberOfCovidTweetsRetrieved', ascending=False)

# TODO sudo pip install nltk -> remove stop words

# Function for the top 10 words in 'text' field for each Country
def top10words_func(df,country):
  df_top10words_partial = df.filter(f.col("country_code")==country) \
    .select(f.explode(f.split("text_concat", ' ')).alias("top10words")) \
    .filter((f.col("top10words") != '')) \
    .groupBy('top10words') \
    .count() \
    .sort('count', ascending=False)
  
  list_of_couples = df_top10words_partial.rdd \
    .map(lambda (a,b): a + ":" + str(b)) \
    .take(10)
  final_top10text = ""
  for word in list_of_couples:
    final_top10text = final_top10text + "(" + word + "),"
  final_top10text_row = Row(country_code=country, day=day, top10words=final_top10text[:-1])
  return final_top10text_row

# Creating the list of Countries
list_of_country = df.select("country_code") \
 .distinct() \
 .rdd \
 .map(lambda row: row[0]) \
 .collect()

# Computing the top 10 words in 'text' field for each Country
final_top10text_list = []
for country in list_of_country:
  final_top10text_list.append(top10words_func(df_groupByCountry_text, country))

# Creating the dataframe for the 10 top words (joining the 2 final dataframes)
rdd_top10words = sc.parallelize(final_top10text_list)
df_top10words_to_join = spark.createDataFrame(rdd_top10words,['country', 'day_t2', 'top10words'])
df_top10words = df_groupByCountry_text \
  .join(df_top10words_to_join, (df_groupByCountry_text.country_code == df_top10words_to_join.country) \
    & (df_groupByCountry_text.day == df_top10words_to_join.day_t2), 'left') \
  .select("country_code", "day", "numberOfCovidTweetsRetrieved", "top10words")


# ==================== Top 10 Words ====================

# group by country,day and concatenating the 'hashtags' field
df_groupByCountry_hashtags = df \
  .groupBy('country_code','day') \
  .agg( \
    f.collect_list('hashtags').alias("hashtags_concat"))

# Function for the top 10 hashtags in 'hashtags' field for each Country
def top10pounds_func(df,country):
  df_top10pounds_partial = df_groupByCountry_hashtags.filter(f.col("country_code")==country) \
    .select(f.explode("hashtags_concat").alias("top10pounds_concat")) \
    .select(f.explode("top10pounds_concat").alias("top10pounds")) \
    .withColumn("top10pounds", f.expr("substring(top10pounds, 2, length(top10pounds)-2)")) \
    .groupBy('top10pounds') \
    .count() \
    .sort('count', ascending=False)
  
  list_of_couples = df_top10pounds_partial.rdd \
    .map(lambda (a,b): a + ":" + str(b)) \
    .take(10)
  final_top10pounds = ""
  for word in list_of_couples:
    final_top10pounds = final_top10pounds + "(" + word + "),"
  final_top10pounds_row = Row(country_code=country, day=day, top10pounds=final_top10pounds[:-1])
  return final_top10pounds_row

# Computing the top 10 hashtags in 'hashtags' field for each Country
final_top10pounds_list = []
for country in list_of_country:
  final_top10pounds_list.append(top10pounds_func(df_groupByCountry_hashtags, country))

# Creating the dataframe for the 10 top hashtags (joining the 2 final dataframes)
rdd_top10pounds = sc.parallelize(final_top10pounds_list)
df_top10pounds_to_join = spark.createDataFrame(rdd_top10pounds,['country', 'day_t2', 'top10pounds'])
df_top10pounds = df_groupByCountry_hashtags \
  .join(df_top10pounds_to_join, (df_groupByCountry_hashtags.country_code == df_top10pounds_to_join.country) \
    & (df_groupByCountry_hashtags.day == df_top10pounds_to_join.day_t2), 'left') \
  .selectExpr("country_code as country", "day as day_t2", "top10pounds")


# ==================== Create the final dataframe joining the 2 top10 dataframes ====================

df_final_partial = df_top10words \
  .join(df_top10pounds, (df_top10words.country_code == df_top10pounds.country) \
    & (df_top10words.day == df_top10pounds.day_t2))

# Ordering the output
df_final = df_final_partial.select("country_code", "day", "numberOfCovidTweetsRetrieved", "top10words", "top10pounds") \
  .orderBy(["day", "numberOfCovidTweetsRetrieved", "country_code"], ascending=[1, 0, 0])

# TODO quality check (query df and df_final)
# Create external Hive table on S3 if not exists
spark.sql("CREATE TABLE IF NOT EXISTS etl2 (country_code STRING, day STRING, numberOfCovidTweetsRetrieved INT, top10words STRING, top10pounds STRING) USING hive LOCATION 's3://test-bucket-premium/emr_covid19_data/'")

# Write the final dataframe as etl2 table in Hive
# repartition 1 since data is small (around 30 KB)
df_final.repartition(1).write.mode("append").format("hive").saveAsTable("etl2")

sc.stop()