from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from  pyspark.sql.functions import regexp_replace, col, split, unix_timestamp, from_unixtime, coalesce, to_date, lit, when
from  pyspark.sql import functions as f
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
from pyspark.sql.utils import AnalysisException 
import boto3
from io import StringIO
spark.sql('set spark.sql.caseSensitive=true')


def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False
     

def formatdate(col, formats=("MM/dd/yyyy", "yyyy-MM-dd", "MM/dd/yy", "MM/dd/y")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])


yesterday = date.today() - timedelta(days=13)
dateformat = yesterday.strftime('%m-%d-%Y')
dateval = dateformat.split('-')
month = dateval[0]
day = dateval[1]
year = dateval[2]


bucket = 's3://github-data-group4'
input_path = '/github/{}/{}/{}-{}-{}.csv'.format(year, month, month, day, year)
lookup_path = 's3://emrproject/lookuptable/lookuptable.csv'


def readgithubdata():
    
    print("{}-{}-{}".format(month,day,year))
    print(bucket+input_path)
    gitdf = sqlContext.read.load(bucket+input_path, 
                          format='csv', 
                          header='true', 
                          inferSchema='true',
                          mode='PERMISSIVE')
    return gitdf


def readlookuptable():
    lookuptable = sqlContext.read.load(lookup_path, 
                          format='csv', 
                          header='true', 
                          inferSchema='true')
    return lookuptable




def processdata():
   
    githubdata = readgithubdata()
    
    #Handle latitude column name format and inset column if not present
    if 'latitude' in githubdata.columns:
        pass
    elif 'Latitude' in githubdata.columns:
        githubdata = githubdata.withColumnRenamed('Latitude', 'latitude')
    elif 'Lat' in githubdata.columns: 
        githubdata = githubdata.withColumnRenamed('Lat', 'latitude')
    else:
        githubdata = githubdata.withColumn('latitude', f.lit(None).cast(StringType()))
    
    #Handle longitude column name format and inset column if not present
    if 'longitude' in githubdata.columns:
        pass
    elif 'Long_' in githubdata.columns:
        githubdata = githubdata.withColumnRenamed('Long_', 'longitude')
    elif 'Longitude' in githubdata.columns: 
        githubdata = githubdata.withColumnRenamed('Longitude', 'longitude')
    else:
        githubdata = githubdata.withColumn('longitude', f.lit(None).cast(StringType()))

    #Insert columns if not present
    columns = ['FIPS', 'Admin2', 'Combined_Key', 'Active']
    for i in columns:
        if has_column(githubdata, i) == False:
            githubdata = githubdata.withColumn(i, lit(None).cast(StringType()))
            
    #Rename Columns
    for col in githubdata.columns:
        githubdata = githubdata.withColumnRenamed(col, col.lower().replace(' ', '_').replace('/', '_'))
    
    #Drop Columns
    drop_lst = ['admin2', 'combined_key']
    df = githubdata.drop(*drop_lst)
    df = df.select('fips', 'province_state', 'country_region', 'last_update', 'latitude', 'longitude', 'confirmed', 'recovered', 'deaths', 'active')
    print ("Showing GITHUB DATA SCHEMA after dropping columns")
    df.printSchema()
    
    #insert month column and format date
    df = df.withColumn("date", formatdate("last_update"))
    df = df.withColumn('month',f.month(df.date))

    #Column value modification
    df = df.withColumn('country_region', regexp_replace('country_region', "Mainland China", "China"))
    df = df.withColumn('country_region',when(df['country_region'].like("%Diamond Princess%") | df['province_state'].like("Diamond%"), 'Diamond Princess' ).otherwise(df['country_region']))
    df = df.withColumn('province_state',when(df['country_region'].like("%Diamond Princess%") | df['province_state'].like("Diamond%"), 'Diamond Princess' ).otherwise(df['province_state']))
    print("Showing values after Modifying Column value for DIAMOND PRINCESS")
    df.where('''country_region == "Diamond Princess"''').show(5)
    print("Showing values after Modifying Column value for CHINA")
    df.where('''country_region == "China"''').show(5)
    return df



def lookuptable():
    
    lookup = readlookuptable()
    
    drop_lookup = ['UID', 'iso3', 'code3', 'FIPS', 'Admin2', 'Lat', 'Long_', 'Combined_Key', 'Population']
    lookup = lookup.drop(*drop_lookup)
    print("Showing LOOKUP TABLE schema after dropping columns")
    lookup.printSchema()
    return lookup


def data(lookupdata, dfdata):
    
    cond1 = (dfdata['province_state'] == lookupdata['Province_State']) & (dfdata['country_region'] == lookupdata['Country_Region'])
    cond2 = (dfdata['province_state'] == None) & (dfdata['country_region'] == lookupdata['Country_Region'])
    df_province = dfdata.join(lookupdata, cond1)
    df_country = dfdata.join(lookupdata, cond2)
    df_country.where("country == 'HK'" and "province_state == 'Hong Kong'" ).show(5)
    df_country.where("country_region == 'India'").show(5)
    
#     df3 = df3.withColumnRenamed('iso2', 'country')
#     data = df3.drop('Country_Region', 'Province_State')
#     data.show(5)
#     print("======================SANITY CHECKS=======================")
#     data.where("country == 'CN'" and "province_state == 'Zhejiang'" ).show(5)
#     data.where("country == 'HK'" and "province_state == 'Hong Kong'" ).show(5)
#     data.where("country == 'GF'" and "province_state == 'French Guiana'" ).show(5)
#     data.where("country_region == 'India'").show(5)
#     data.where("country_region == 'Diamond Princess'").show(5)
# #     data.where("country == 'DP'").show(5)
#     data.sort('country_region', ascending=False).show(5)
#     data.sort('fips', ascending=True).show(5)
#     return data

df = processdata()
lookup = lookuptable()
data(lookup, df)
