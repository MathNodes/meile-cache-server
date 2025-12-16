#!/bin/env python3

import requests
import scrtsxx
import pymysql
from requests.exceptions import ReadTimeout
import os
import configparser
from time import sleep

VERSION   = "3.0"
STARTPAGE = 1
FIRSTKEY  = "AAAAAAAPnos="
delta     = 314
BASEDIR   = os.path.join(os.path.expanduser('~'), '.meile-cache')
CONFFILE  = os.path.join(BASEDIR,'config.ini')
CONFIG    = configparser.ConfigParser()

API = "https://api.sentinel.mathnodes.com/"
ENDPOINT = "sentinel/subscription/v3/subscriptions?pagination.key=%s&pagination.limit=1000"

SUBTYPES = {'node' : '/sentinel.subscription.v2.NodeSubscription', 'plan' : '/sentinel.subscription.v2.PlanSubscription'}

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
    NEXTKEY = []
    NEXTKEY.append(CONFIG['subscriptions'].get('next_key', FIRSTKEY))
    print(NEXTKEY[-1])
    sleep(4)
    page_range = list(range(STARTPAGE,STARTPAGE+delta))
    k = 0
    while k < len(page_range):
        if k == 0:
            APIURL = "https://api.sentinel.mathnodes.com/sentinel/subscription/v3/subscriptions?pagination.limit=1000"
        else:
            APIURL = API + ENDPOINT % NEXTKEY[-1] 
        print("Getting page: %s" % page_range[k])
        try: 
            r = requests.get(APIURL, timeout=60)
            subJSON = r.json()
            sleep(4)
        except ReadTimeout:
            print("ERROR: ReadTimeout... Retrying...")
            continue
        try:
            if subJSON['subscriptions']:
                for sub in subJSON['subscriptions']:
                    try: 
                        #subtype    = sub['@type']
                        ID         = int(sub['id'])
                        subscriber = sub['acc_address']
                        plan_id    = int(sub['plan_id'])
                        denom      = sub['price']['denom']
                        base_value = float(sub['price']['base_value'])
                        quote_value = int(sub['price']['quote_value'])
                        policy     = sub['renewal_price_policy']
                        status     = sub['status']
                        inactive   = sub['inactive_at']
                        sub_date   = sub['start_at']

                    except Exception as e:
                        print(str(e))
                        continue
                    
                    
                    
                    #print("%s,%s,%s,%s,%s" % (ID,subscriber,node,price,sub_date))
                    
                    iquery = '''
                    INSERT IGNORE INTO subscriptions (id,owner,deposit,denomination,sub_date, inactive_date,type,plan_id,policy,base_value)
                    VALUES
                    (%d,"%s",%d,"%s","%s","%s","plan",%d,"%s",%.18f)
                    ''' % (ID,subscriber,quote_value,denom,sub_date,inactive,plan_id,policy,base_value)
                    
                   
                    print(iquery)
                    InsertIntoSubTable(iquery,db)
                k += 1
                try: 
                    NEXTKEY.append(subJSON['pagination']['next_key'])
                    if NEXTKEY[-1] == "null":
                        WriteConfig(CONFIG, CONFFILE, NEXTKEY)
                        break
                    
                except Exception as e:
                    print(str(e))
                    WriteConfig(CONFIG, CONFFILE, NEXTKEY)
                    break
            else:
                WriteConfig(CONFIG, CONFFILE, NEXTKEY)
                break
        except KeyError:
            WriteConfig(CONFIG, CONFFILE, NEXTKEY)
            break

def WriteConfig(CONFIG, CONFFILE, NEXTKEY):
    
    print("Found the end. Saving and exiting...", end='')
    sleep(2)
    CONFIG.set('subscriptions', 'next_key', NEXTKEY[-2])
    FILE = open(CONFFILE, 'w')
    CONFIG.write(FILE)
    FILE.close()
    print("Done.")

if __name__ == "__main__":
    read_configuration(CONFFILE)
    db = connDB()
    GetSubscriptionAndPopulateDB(db)
