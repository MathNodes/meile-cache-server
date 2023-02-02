#!/bin/env python3

import requests
import scrtsxx
import pymysql
from requests.exceptions import ReadTimeout
import os
import configparser
from time import sleep


STARTPAGE = 6357
delta     = 100
BASEDIR   = os.path.join(os.path.expanduser('~'), '.meile-cache')
CONFFILE  = os.path.join(BASEDIR,'config.ini')
CONFIG    = configparser.ConfigParser()


def read_configuration(confpath):
    CONFIG.read(confpath)
    return CONFIG

def connDB(): 
    db = pymysql.connect(host=scrtsxx.HOST,
                         port=scrtsxx.PORT,
                         user=scrtsxx.USERNAME,
                         passwd=scrtsxx.PASSWORD,
                         db=scrtsxx.DB,
                         charset="utf8mb4",
                         cursorclass=pymysql.cursors.DictCursor
                         )

    return db

def InsertIntoSubTable(query,db):
    try: 
        c = db.cursor()
        c.execute(query)
        db.commit()
        return 0
    except Exception as e:
        print(str(e))
        return 1

def GetSubscriptionAndPopulateDB(db):
    STARTPAGE = int(CONFIG['subscriptions'].get('startpage', 6357))
    page_range = list(range(STARTPAGE,STARTPAGE+delta))
    k = 0
    while k < len(page_range):
        APIURL = "https://api.sentinel.mathnodes.com/subscriptions?page=%s" % page_range[k]
        print("Getting page: %s" % page_range[k])
        k += 1
        try: 
            r = requests.get(APIURL, timeout=60)
            subJSON = r.json()
            sleep(2)
        except ReadTimeout:
            print("ERROR: ReadTimeout... Retrying...")
            k = k - 1
            continue
        if subJSON['result']['subscriptions']:
            for sub in subJSON['result']['subscriptions']:
                try: 
                    ID         = sub['id']
                    subscriber = sub['owner']
                    node       = sub['node']
                    deposit    = sub['price']['amount']
                    denom      = sub['price']['denom']
                    sub_date   = sub['status_at']
                except Exception as e:
                    print(str(e))
                    continue
                
                
                #print("%s,%s,%s,%s,%s" % (ID,subscriber,node,price,sub_date))
                
                iquery = '''
                INSERT IGNORE INTO subscriptions (id,owner,node,deposit,denomination,sub_date)
                VALUES
                (%d,"%s","%s",%d,"%s","%s")
                ''' % (int(ID),subscriber,node,int(deposit),denom,sub_date)
                
                InsertIntoSubTable(iquery,db)
        else:
            print("Found the end. Saving and exiting...", end='')
            sleep(2)
            # Backoff by 2. Re-reading one page is fine as we ignore any inserts.
            # Plus the last page we successfully read may not be completed and contain more 
            # subscription data on the next run.
            CONFIG.set('subscriptions', 'startpage', str(page_range[k-2]))
            FILE = open(CONFFILE, 'w')
            CONFIG.write(FILE)
            FILE.close()
            print("Done.")
            break
            
            
            
        


if __name__ == "__main__":
    read_configuration(CONFFILE)
    db = connDB()
    GetSubscriptionAndPopulateDB(db)