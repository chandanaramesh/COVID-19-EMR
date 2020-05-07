import sys
from operator import add
from pyspark import SparkConf, SparkContext
from datetime import date, timedelta
import wget
import os
# for SparkConf() check out http://spark.apache.org/docs/latest/configuration.html
conf = (SparkConf()
        .setMaster("local")
        .setAppName("git-data-previous-days-download")
        .set("spark.executor.memory", "1g"))
sc = SparkContext(conf=conf)

def daterange(start_date, end_date): # generate range of date 
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
        
start_date = date(2020, 1, 22) # starting date from the github
end_date = date.today()



print("Launch App..")
if __name__ == "__main__":
    print("Initiating main..")
    print('Beginning file download of all last days')
    os.system("aws s3 ls github-data-group4/github --recursive| awk '{ if($3 >0) print $4}' > existing_files.txt")
    existing_files=[]
    with open('existing_files.txt') as f:
        lines = f.readlines()
        existing_files=[l.split('/')[-1][:-5] for l in lines]# remove extention and prefix
        print(lines)
        print(existing_files)
    for single_date in daterange(start_date, end_date):
        yesterday=single_date.strftime("%m-%d-%Y")
        month,day,year=yesterday.split('-')
        if yesterday in existing_files:
            print ('file existed already')
            continue
        url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + yesterday + '.csv'
        try:
            wget.download(url, yesterday + '.csv')
            # uploading to s3
            os.system('aws s3 cp ' + yesterday + '.csv s3://github-data-group4/github/'+year+'/'+month+'/')
            os.system('rm ' + yesterday + '.csv')
        except:
            print(' missing csv file on the '+str(yesterday))
            continue   
            
    sc.stop()
