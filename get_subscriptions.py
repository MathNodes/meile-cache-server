#!/bin/env python3

import requests
import scrtsxx
import pymysql
from requests.exceptions import ReadTimeout
import os
import configparser
from time import sleep

VERSION   = "2.0"
STARTPAGE = 1
FIRSTKEY  = "AAAAAAADNfs="
delta     = 100
BASEDIR   = os.path.join(os.path.expanduser('~'), '.meile-cache')
CONFFILE  = os.path.join(BASEDIR,'config.ini')
CONFIG    = configparser.ConfigParser()

API = "https://api.sentinel.mathnodes.com/"
ENDPOINT = "sentinel/subscriptions?pagination.key=%s&pagination.limit=1000"

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
                        subtype    = sub['@type']
                        ID         = sub['base']['id']
                        subscriber = sub['base']['address']
                        inactive   = sub['base']['inactive_at']
                        sub_date   = sub['base']['status_at']
                        
                        for key,value in SUBTYPES.items():
                            if value in subtype:
                                subtype = key
                        
                        if subtype == "plan":
                            plan_id = sub['plan_id']
                            denom   = sub['denom']
                        else:
                            node       = sub['node_address']
                            gb         = sub['gigabytes']
                            hours      = sub['hours']
                            denom      = sub['deposit']['denom']
                            deposit    = sub['deposit']['amount']
                            
                    except Exception as e:
                        print(str(e))
                        continue
                    
                    
                    
                    #print("%s,%s,%s,%s,%s" % (ID,subscriber,node,price,sub_date))
                    if subtype == "node":
                        iquery = '''
                        INSERT IGNORE INTO subscriptions (id,owner,node,deposit,denomination,sub_date, inactive_date,type,gigabytes,hours)
                        VALUES
                        (%d,"%s","%s",%d,"%s","%s","%s","%s",%.3f,%.3f)
                        ''' % (int(ID),subscriber,node,int(deposit),denom,sub_date,inactive,subtype,float(gb),float(hours))
                    else:
                        iquery = '''
                        INSERT IGNORE INTO subscriptions (id,owner,denomination,sub_date,inactive_date,type,plan_id)
                        VALUES
                        (%d,"%s","%s","%s","%s","%s",%d)
                        ''' % (int(ID),subscriber,denom,sub_date,inactive,subtype,int(plan_id))
                    print(iquery)
                    InsertIntoSubTable(iquery,db)
                k += 1
                try: 
                    NEXTKEY.append(subJSON['pagination']['next_key'])
                    if NEXTKEY[-1] == "null":
                        WriteConfig(CONFIG, CONFFILE, NEXTKEY)
                    
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
