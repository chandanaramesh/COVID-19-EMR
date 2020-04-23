import sys
from operator import add
from pyspark import SparkConf, SparkContext
from datetime import date, timedelta
import wget
import os
# for SparkConf() check out http://spark.apache.org/docs/latest/configuration.html
conf = (SparkConf()
        .setMaster("local")
        .setAppName("git-data-download")
        .set("spark.executor.memory", "1g"))
sc = SparkContext(conf=conf)

print("Launch App..")
if __name__ == "__main__":
    print("Initiating main..")
    print('Beginning file download of last day data')

    yesterday = str((date.today() - timedelta(days=1)).strftime('%m-%d-%Y'))
    month,day,year=yesterday.split('-')
    print(day)
    url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + yesterday + '.csv'
    print(url)
    wget.download(url, yesterday + '.csv')
    # uploading to s3
    os.system('aws s3 cp ' + yesterday + '.csv s3://github-data-group4/github/'+year+'/'+month+'/')
    # for lambdas check out https://docs.python.org/3/tutorial/controlflow.html#lambda-expressions
    sc.stop()
