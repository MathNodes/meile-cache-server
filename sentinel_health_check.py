#!/bin/env python3

import pymysql
import scrtsxx
import requests

VERSION = 20240309.010146

HEALTH_CHECK_URL = 'https://api.health.sentinel.co/v1/records'

class SentinelHealthCheck():
    
    def connDB(self): 
        db = pymysql.connect(host=scrtsxx.HOST,
                             port=scrtsxx.PORT,
                             user=scrtsxx.USERNAME,
                             passwd=scrtsxx.PASSWORD,
                             db=scrtsxx.DB,
                             charset="utf8mb4",
                             cursorclass=pymysql.cursors.DictCursor
                             )
    
        return db
    
    
    def get_health_check_and_store(self, db):
        c = db.cursor()
        query = "DROP TABLE health_check;"
        c.execute(query)
        db.commit()
        
        query = "CREATE TABLE health_check (node_address VARCHAR(100), config_exchange_timestamp TIMESTAMP, config_exchange_error VARCHAR(2000), info_fetch_timestamp TIMESTAMP, info_fetch_error VARCHAR(2000), location_fetch_timestamp TIMESTAMP, location_fetch_error VARCHAR(2000), ok BOOLEAN, status TINYINT, PRIMARY KEY(node_address));"
        c.execute(query)
        db.commit()
        
        r = requests.get(HEALTH_CHECK_URL) #specify a constant in konstants.py
        data = r.json()

        for nodehealthdata in data['result']:
            if "info_fetch_error " in nodehealthdata:
                ife = nodehealthdata['info_fetch_error']
            else:
                ife = ""
            if "config_exchange_error" in nodehealthdata:
                cee = nodehealthdata['config_exchange_error']
            else:
                cee = ""
            if "location_fetch_error" in nodehealthdata:
                lfe = nodehealthdata['location_fetch_error']
            else:
                lfe = ""
            if "ok" in nodehealthdata:
                ok = nodehealthdata['ok']
            else:
                ok = ""
                
            query = '''
            INSERT IGNORE INTO health_check
            (node_address,
            config_exchange_timestamp, 
            config_exchange_error, 
            info_fetch_timestamp, 
            info_fetch_error, 
            location_fetch_timestamp, 
            location_fetch_error, 
            ok, 
            status)
            VALUES ("%s", "%s", '%s', "%s", '%s', "%s", '%s', "%s", %d);
            ''' % (nodehealthdata['addr'],
                   nodehealthdata['config_exchange_timestamp'],
                   cee,
                   nodehealthdata['info_fetch_timestamp'],
                   ife,
                   nodehealthdata['location_fetch_timestamp'],
                   lfe,
                   ok,
                   int(nodehealthdata['status'] if 'status' in nodehealthdata else 0))
            
            print(query)
            c.execute(query)
            db.commit()
    
if __name__ == "__main__":
    shc = SentinelHealthCheck()
    shc.get_health_check_and_store(shc.connDB())
    