'''
Created on 27-Mar-2019

@author: sripradeepp
'''

from kafka import KafkaConsumer
import redis
import json

r = redis.Redis(host='localhost', port=6379, db=0)
consumer = KafkaConsumer(
    'bitcoin',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True)

for message in consumer:
    
    # maintains the latest 100 transactions in the list named 'latest_trans'
    r.lpush("latest_trans",message.value)
    r.ltrim("latest_trans", 0, 99)
    
    
    #maintain the address and agregate value in a sorted set 'address_value_set'
    jsonMessage = json.loads(message.value)
    time = jsonMessage["x"]["time"]
    hrs3 = time + 3*60*60
    for out in jsonMessage["x"]["out"]:
        if out["addr"] is not None:
            key = out["addr"]+"_"+str(hrs3)
            if r.zscore("address_value_set", key) is not None:
                print("inside if ")
                val = out["value"] + r.zscore("address_value_set", key) #will add the value if the address comes in the same minute
            else:
                val = out["value"]    
            
            itemTemp = {key:val}
            r.zadd("address_value_set", itemTemp)

    #Maintain the transaction count details in  'trans_count' hashmap
    if r.hget("trans_count",time) is not None:
        r.hset("trans_count",time,int(r.hget("trans_count",time).decode())+1)
    else:
        r.hset("trans_count", time, 1)

if __name__ == '__main__':
    pass