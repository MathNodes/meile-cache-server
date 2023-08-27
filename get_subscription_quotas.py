#!/usr/bin/env python3
import pymysql
import scrtsxx
import requests
from time import sleep
from timeit import default_timer as timer

VERSION = 2.1
API = "https://api.sentinel.mathnodes.com"
GB  = 1000000000
QUOTAID = 196212


def connDB():
    db = pymysql.connect(host=scrtsxx.HOST,
                         port=scrtsxx.PORT,
                         user=scrtsxx.USERNAME,
                         passwd=scrtsxx.PASSWORD,
                         db=scrtsxx.DB,
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    return db


def GetSubscriptionTable(db):
    
    query = "SELECT * from subscriptions"
    c = db.cursor()
    c.execute(query)
    
    return c.fetchall()

def GetSubIDLimit(db):
    
    query = "select * from subscriptions where sub_date >= '2023-08-18 12:10:42' ORDER BY ID ASC LIMIT 1;"
    c = db.cursor()
    c.execute(query)
    
    return c.fetchone()

def GetQuotaFromAPI(db, s, idlimit):
    
    if idlimit is not None:
        QUOTAID = int(idlimit['id'])
    datapoints = 1
    for row in s:
        if int(row['id']) < QUOTAID:
            continue
        endpoint = "/sentinel/subscriptions/%s/allocations" % (row['id'])
        try: 
            r = requests.get(API + endpoint, timeout=15)
            subJSON = r.json()
            #print(subJSON)
            sleep(1.314)
        except Exception as e:
            print(str(e))
            continue
        
        try:
            if len(subJSON['allocations']) > 0:
                allocated = round(float(float(subJSON['allocations'][0]['granted_bytes']) / GB),5)
                consumed  = round(float(float(subJSON['allocations'][0]['utilised_bytes']) /GB),5)
            else:
                allocated = consumed = 0.0
        except Exception as e:
            print(str(e))
            continue
        
        query = '''INSERT INTO subscription_quotas (id,allocated,consumed)
                   VALUES (%d, "%.5f", "%.5f")
                   ON DUPLICATE KEY UPDATE
                   id=%d,allocated=%.5f,consumed=%.5f
                ''' % (int(row['id']), allocated, consumed,
                       int(row['id']), allocated, consumed)
        #print(query)
        try:         
            c = db.cursor()
            c.execute(query)
            db.commit()
        except Exception as e:
            print(str(e))
            continue
        
        datapoints += 1
        
    return datapoints
        
        
            
        
            

if __name__ == "__main__":
    db = connDB()
    start = timer()
    subTable = GetSubscriptionTable(db)
    subIDLimit = GetSubIDLimit(db)
    end = timer()
    
    time1 = round((end-start),4)
    print("It took %ss to get database tables" % (time1))
    #print(subIDLimit)
    start = timer()
    datapoints = GetQuotaFromAPI(db, subTable, subIDLimit)
    end = timer()
    
    time2 = round((end-start),4)
    
    print("It took %ss to get %d quota subscription data points" % (time2, datapoints))
    
    