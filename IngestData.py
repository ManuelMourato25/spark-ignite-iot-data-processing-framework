import requests
from kafka import SimpleProducer, SimpleClient
from datetime import datetime,timedelta
from sys import  argv

# This script calls sensor data from the following API: 'http://uoweb1.ncl.ac.uk/api/v1/sensors/data/raw.json'
#This API provides realtime data in JSON format of multiple sensors and their readings

#A beginning and end dates need to be supplied in the following format: yymmddHHMMSS

date_begin="20170901003930"
kafka = SimpleClient("localhost:9092")
producer = SimpleProducer(kafka)
source='http://uoweb1.ncl.ac.uk/api/v1/sensors/data/raw.json'
#You can create this API key at http://uoweb1.ncl.ac.uk/api
api_key=argv[1]
date_start=datetime.strptime(date_begin,"%Y%m%d%H%M%S")
date_end=date_start+timedelta(0,2)

while(True):
    start_time_str=date_start.strftime("%Y%m%d%H%M%S")
    end_time_str=date_end.strftime("%Y%m%d%H%M%S")
    raw_json=requests.get(source,
                      'api_key='+api_key+'&start_time='+start_time_str+'&end_time='+end_time_str)
    sensorData=raw_json.json()

    # Sends data to a Kafka Consumer, which in turn will be the producer
    #for the Spark Streaming Application
    producer.send_messages("sensorData",str(sensorData))
    print "Message sent to Kafka:From "+start_time_str+" to "+end_time_str
    date_start=date_end
    date_end=date_end+timedelta(0,5)


