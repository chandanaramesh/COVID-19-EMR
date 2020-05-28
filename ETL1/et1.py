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
    
    return coalesce(*[to_date(col, f) for f in formats]) 


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


#Reading all columns 
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
    drop_lst = ['latitude', 'longitude']
    df = githubdata.drop(*drop_lst)
    df = df.select('fips', 'combined_key', 'province_state', 'country_region', 'last_update', 'confirmed', 'recovered', 'deaths', 'active')
    print ("Showing GITHUB DATA SCHEMA after dropping columns")
    df.printSchema()
    df.show(5)
    
    
    #insert month column and format date
    df = df.withColumn("date", formatdate("last_update"))
    df = df.withColumn('date', regexp_replace('date', "00", "20"))
    df = df.withColumn('month',f.month(df.date))

    #Column value modification
    df = df.withColumn('country_region', regexp_replace('country_region', "Mainland China", "China"))
    df = df.withColumn('country_region', regexp_replace('country_region', "South Korea", "Korea, South"))
    df = df.withColumn('country_region', regexp_replace('country_region', "Taiwan\*", "Taiwan*"))
    df = df.withColumn('combined_key', regexp_replace('combined_key', "Taiwan\*", "Taiwan*"))
    #Replacing , or ,, in combined_key column
    df = df.withColumn('combined_key', regexp_replace('combined_key', "^,+", ""))
    #Replacing strings like Hardin,Ohio,US to Hardin, Ohio, US
    df = df.withColumn('combined_key', regexp_replace('combined_key', ",(?=\S)", ", "))
    #Replace strings that start with a whitespace
    df = df.withColumn('combined_key', regexp_replace('combined_key', "^\s*", ""))
    #Replace Diamon Princess in combined_key
    df = df.withColumn('combined_key', regexp_replace('combined_key', "Diamond Princess, Cruise Ship*", "Diamond Princess"))
    
    df = df.withColumn('combined_key', regexp_replace('combined_key', " County,", ","))
    df = df.withColumn('combined_key', regexp_replace('combined_key', "^unassigned", "Unassigned"))
    df = df.withColumn('combined_key', regexp_replace('combined_key', "^Unknown, ", ""))
    df = df.withColumn('combined_key', regexp_replace('combined_key', "Doña Ana, New Mexico, US", "Dona Ana, New Mexico, US"))
    df = df.withColumn('combined_key',when(df['combined_key'].like('District of Columbia%'), 'District of Columbia, US' ).otherwise(df['combined_key']))
    df = df.withColumn('country_region',when(df['country_region'].like("%Diamond Princess%") | df['province_state'].like("Diamond%"), 'Diamond Princess' ).otherwise(df['country_region']))
    df = df.withColumn('province_state',when(df['country_region'].like("%Diamond Princess%") | df['province_state'].like("Diamond%"), None ).otherwise(df['province_state']))
    df = df.withColumn('country_region',when(df['country_region'] == 'Hong Kong', 'China' ).otherwise(df['country_region']))
    df = df.withColumn('country_region',when(df['country_region'] == 'Macau', 'China' ).otherwise(df['country_region']))
    df = df.withColumn('province_state',when(df['country_region'] == df['province_state'], None ).otherwise(df['province_state']))

    print("COUNTING DF")
    print(df.count())
    return df


def lookuptable():
    
    lookup = readlookuptable()
    
    drop_lookup = ['UID', 'iso3', 'code3', 'FIPS', 'Admin2', 'Lat', 'Long_', 'Population']
    lookup = lookup.drop(*drop_lookup)
    return lookup


def data(lookupdata, dfdata):
    
    cond = (dfdata['combined_key'] == lookupdata['Combined_Key'])
    
    df_final = dfdata.join(lookupdata, cond).drop('Combined_Key')
    #Drop Columns  
    df_final = df_final.withColumnRenamed('iso2', 'country')
    df_final = df_final.drop('Country_Region', 'Province_State', 'last_update')
    df_final.where("combined_key = 'Veneto, Italy'").show()
    print("======================SANITY CHECKS=======================")
    df_final.where("country_region == 'Diamond Princess'").show(5)
    print("COUNTING FINAL DF")
    print(df_final.count())
    print("Printing differences")
    df.select('combined_key').subtract(df_final.select('combined_key')).show(100)
    df_final.show()
    return df_final

df = processdata()
lookup = lookuptable()
datavalue = data(lookup, df)


def writefile(finaldata):
    
    output_file = '/githubdata/'
    finaldata.write.partitionBy("country", "month").format("parquet").mode('append').option("path", "s3://emrproject/githubdata/").saveAsTable('github_daily', mode='append')
    print(bucket+input_path)
    spark.sql("SELECT DISTINCT(date) FROM github_daily ORDER BY date DESC").show()
    
writefile(datavalue)

