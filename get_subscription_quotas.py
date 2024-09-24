#!/usr/bin/env python3
import pymysql
import scrtsxx
import requests
from time import sleep
from timeit import default_timer as timer
import concurrent.futures

VERSION = 20240923.2044
API = "https://api.sentinel.mathnodes.com"
GB  = 1000000000
QUOTAID = 196212


class BandwidthUsage():
    Bandwidth = []
    def __init__(self):
        
        self.db = pymysql.connect(host=scrtsxx.HOST,
                             port=scrtsxx.PORT,
                             user=scrtsxx.USERNAME,
                             passwd=scrtsxx.PASSWORD,
                             db=scrtsxx.DB,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        '''
        self.db_pool = PooledDB(
                creator=pymysql, 
                maxconnections=5, 
                host=scrtsxx.HOST, 
                port=scrtsxx.PORT,
                user=scrtsxx.USERNAME,
                passwd=scrtsxx.PASSWORD,
                database=scrtsxx.DB,      
            )
        '''
    
    
    def GetSubscriptionTable(self):
        
        query      = "SELECT * from subscriptions"
        c          = self.db.cursor()
        c.execute(query)
        
        return c.fetchall()
    
    def GetSubIDLimit(self):
        
        # Date of Sentinel Upgrade to hourly subscriptions
        query      = "select * from subscriptions where sub_date >= '2023-08-18 12:10:42' ORDER BY ID ASC LIMIT 1;"
        c          = self.db.cursor()
        c.execute(query)
        
        return c.fetchone()
    
    def GetQuotaFromAPI(self, s, idlimit):
        
        if idlimit is not None:
            self.QUOTAID = int(idlimit['id'])
        datapoints = 1
        
        c = self.db.cursor()
            
        self.__api_quota_multithread(s)
        
        datapoints = 1
        for sub in self.Bandwidth:
            query = '''INSERT INTO subscription_quotas (id,allocated,consumed)
                       VALUES (%d, "%.5f", "%.5f")
                       ON DUPLICATE KEY UPDATE
                       id=%d,allocated=%.5f,consumed=%.5f
                    ''' % (int(sub['id']), sub['allocated'], sub['consumed'],
                           int(sub['id']), sub['allocated'], sub['consumed'])
        
            try:         
                c.execute(query)
                self.db.commit()
            except Exception as e:
                print(str(e))
                continue
            
            datapoints += 1
            
        return datapoints
        
        
    def __api_quota_multithread(self, subData):
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            # Submit tasks in batches of 3
            futures = [executor.submit(self.__api_url_worker, s) for s in subData]
    
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # This will raise any exceptions that occurred in the thread
                except Exception as e:
                    print("An error occurred:", str(e))
                    
    def __api_url_worker(self, sub):
        Quota = {}
        if sub['id'] < self.QUOTAID:
            return
        if sub['gigabytes'] > 0.0:
            endpoint = f"/sentinel/subscriptions/{sub['id']}/allocations"  
            #print(API+endpoint)
            try: 
                r = requests.get(API + endpoint, timeout=15)
                subJSON = r.json()
            except Exception as e:
                print(str(e))
                return
            
            try:
                if len(subJSON['allocations']) > 0:
                    allocated = round(float(float(subJSON['allocations'][0]['granted_bytes']) / GB),5)
                    consumed  = round(float(float(subJSON['allocations'][0]['utilised_bytes']) /GB),5)
                else:
                    allocated = consumed = 0.0
            except Exception as e:
                print(str(e))
                return
            
            Quota['id'] = sub['id']
            Quota['allocated'] = allocated
            Quota['consumed'] = consumed
            
            self.Bandwidth.append(Quota)
    
if __name__ == "__main__":
    bu = BandwidthUsage()
    start = timer()
    subTable = bu.GetSubscriptionTable()
    subIDLimit = bu.GetSubIDLimit()
    end = timer()
    
    time1 = round((end-start),4)
    print("It took %ss to get database tables" % (time1))
    #print(subIDLimit)
    start = timer()
    datapoints = bu.GetQuotaFromAPI(subTable, subIDLimit)
    end = timer()
    
    time2 = round((end-start),4)
    
    print("It took %ss to get %d quota subscription data points" % (time2, datapoints))
    
    