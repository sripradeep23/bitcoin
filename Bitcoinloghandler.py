'''
Created on 27-Mar-2019

@author: sripradeepp
'''
from flask import Flask
import redis
import time
import operator
from flask.helpers import make_response
import json
from collections import OrderedDict
from datetime import datetime

r = redis.Redis(host='localhost', port=6379, db=0)

app = Flask(__name__)

def sort_values(rsp):
    return OrderedDict(sorted(rsp.items(), key=operator.itemgetter(1),reverse=True))

def clean():
    currtime = time.time()
    rsp={}
    for i in r.zrange("address_value_set", 0, -1, desc=True,withscores=True):
        keys = i[0].decode("UTF-8").split("_")
        if float(keys[1]) < currtime:
            r.zrem("address_value_set",i[0].decode("UTF-8"))
        elif keys[0] in rsp:                      # getting the actual aggregate for the addresses
            rsp[keys[0]] = int(rsp[keys[0]]) + i[1]
        else:
            rsp[keys[0]] = i[1] 
    
    return json.dumps(sort_values(rsp))        

@app.route('/bitcoin/log/show_transactions')
def showtransaction():
    lst = r.lrange("latest_trans",0,99)
    respdict ={}
    j=0
    for i in lst:
        respdict[j]=json.loads(i.decode("UTF-8"))
        j+=1
    toRet = make_response(json.dumps(respdict))
    toRet.mimetype = 'application/json'
    return toRet    

@app.route('/bitcoin/log/high_value_addr')
def highvalueaddr():
    response = clean()
    toRet = make_response(response)
    toRet.mimetype = 'application/json'
    return toRet

@app.route('/bitcoin/log/transactions_count_per_minute')
def transactions_count_per_minutetest():
    toRet={}
    currtime = time.time() - 1*60*60
    for i in r.hkeys("trans_count"):
        tempTime = float(i.decode("UTF-8"))
        if tempTime < currtime:
            r.hdel("trans_count",str(tempTime))
        else:
            timekey = datetime.fromtimestamp(int(tempTime)).strftime('%H:%M')
            print("time key: ",timekey)
            if timekey in toRet:
                print(toRet[timekey])
                toRet[timekey] = str(int(toRet[timekey])+1)
            else:
                toRet[timekey]=r.hget("trans_count", i).decode("UTF-8")
    res = make_response(json.dumps(toRet))
    res.mimetype = 'application/json'
    return res

if __name__ == '__main__':
    app.run(debug="True")