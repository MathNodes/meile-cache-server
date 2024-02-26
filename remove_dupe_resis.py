#!/bin/env python3
import json
import pymysql
import scrtsxx
import requests
import socket
from contextlib import closing
from timeit import default_timer as timer

VERSION = 2.0
APIURL = 'https://api.sentinel.mathnodes.com'

class DupeResis():
    
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
    
    
    def get_resi_node_list(self, db):
        query = 'SELECT node_address FROM node_score WHERE isp_type = "residential";'
        
        c = db.cursor()
        c.execute(query)
        
        return c.fetchall()
        
        
    def get_remote_url_of_resi_nodes(self, resi_nodes, db):
        c = db.cursor()
        ResiNodeURLs = {}
        for rn in resi_nodes:
            address = rn['node_address']
            query = f'SELECT remote_url FROM node_uptime WHERE node_address = "{address}";'
            #print(query)
            c.execute(query)
            try:
                rurl = c.fetchone()['remote_url'].split("//")[1].split(":")[0]
            except TypeError:
                continue
            
            ResiNodeURLs[address] = rurl
            #print(rurl)

        flipped = {}

        for key, value in ResiNodeURLs.items():
            if value not in flipped:
                flipped[value] = [key]
            else:
                flipped[value].append(key)
        #dupe_resis = [i_key for i_key, i_val in ResiNodeURLs.items() if list(ResiNodeURLs.values()).count(i_val) > 1]
        #print(dupe_resis)
        #print(json.dumps(flipped, indent=4))  
        
        for f_key in flipped.keys():
            if len(flipped[f_key]) > 2:
                for naddress in flipped[f_key]:
                    query = f'UPDATE node_score SET isp_type = "hosting" WHERE node_address = "{naddress}";'
                    print(query)
                    c.execute(query)
                    db.commit()
        
if __name__ == "__main__":
    
    dr = DupeResis()
    db = dr.connDB()
    resi_nodes = dr.get_resi_node_list(db)
    dr.get_remote_url_of_resi_nodes(resi_nodes, db)
    