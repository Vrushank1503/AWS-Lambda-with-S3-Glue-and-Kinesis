import json
import boto3
import random
import time
import datetime
import yfinance as yf 
from time import sleep

def lambda_handler(event, context):
    start = time.time()
    kinesis = boto3.client('kinesis', "us-east-1")
    
    begin = '2021-12-09'
    stop  = '2021-12-10'
    
    tickers = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']
    interval = "5m"
    
    rowcount = 0 
    
    for tick in tickers:
        stock = yf.Ticker(tick)
        data = stock.history(start=begin, end=stop, interval = interval)
        
        total_data ={} 
        for x,y in data.iterrows():
            
            kinesis_record = {
            "high":y["High"],
            "low":y["Low"], 
            "ts":x.strftime('%Y-%m-%d %H:%M:%S'), 
            "name":tick
            }
    
            total_data = json.dumps(kinesis_record)+"\n"
            
            print(total_data) 
            
            kinesis.put_record(
                    StreamName="STA9760F2021_stream",
                    Data=total_data.encode('utf-8'),
                    PartitionKey="partitionkey")
            sleep(0.5)
            
            rowcount = rowcount + 1
    
    end = time.time()
    
    print("The total runtime: " + str(end - start))        
    print(f"The total number of records sent to Kinesis: {rowcount}")       
    
    return {
        'statusCode': 200,
        'body': json.dumps("Run Complete!")
    }      
