#!/bin/env python3

import requests
import random
import scrtsxx
import socket
import pymysql
import ipaddress
from time import sleep 

APIKEYS = scrtsxx.IP_REGISTRY_API_KEYS

VERSION = 20240109.1001
APIURL = 'https://api.sentinel.mathnodes.com'

IPREGISTRY_URL = "https://api.ipregistry.co/%s?key=%s"

class UpdateNodeType():
    
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
    
    def get_node_type_table(self,db):
        
        query = "SELECT * FROM node_score;"
        c = db.cursor()
        c.execute(query)
        
        return c.fetchall()
    
    
    def get_ip_of_node(self, db, NodeData):
        NodeIP = {}
        
        c = db.cursor()

        for n in NodeData:
            address = n['node_address']
            endpoint = APIURL + '/sentinel/nodes/' + address

            # Retrieve remote_url from the table for nodes that have it stored
            query = f"SELECT remote_url FROM node_uptime WHERE node_address = '{address}';"
            c.execute(query)
            result = c.fetchone()
            #print(result['remote_url'])
            if not result['remote_url'] or result['remote_url'] == '':
                
                endpoint = APIURL + '/nodes/' + address
                remote_url = result['remote_url'].split('//')[-1].split(':')[0]
                #print(f"Getting remote_url of: {address}", end=":")
                
                try:
                    r = requests.get(endpoint)
                    remote_url = r.json()['node']['remote_url'].split('//')[-1].split(':')[0]
                except Exception as e:
                    print(str(e))
                    continue
                #print(f"{remote_url}")
            else:
                remote_url = result['remote_url'].split('//')[-1].split(':')[0]
                
            NodeIP[n['node_address']] = remote_url
            try: 
                NodeIP[n['node_address']] = ipaddress.ip_address(remote_url)
            except ValueError:
                try:
                    NodeIP[n['node_address']] = socket.gethostbyname(remote_url)
                except socket.gaierror:
                    continue
            #print(f"{n['node_address']},{NodeIP[n['node_address']]}")
        #print(NodeRemoteURL)
        
        return NodeIP
    
    def query_ipregistry_co(self, db,NodeIP):
        for node,ip in NodeIP.items():
            N = random.randint(0,len(APIKEYS)-1)
            API_KEY = APIKEYS[N]
            TYPE = {"residential" : False, "business" : False, "hosting" : False, "education" : False, "government" : False }
            resp = requests.get(IPREGISTRY_URL % (ip,API_KEY))
            rJSON = resp.json()
            try:
                if rJSON['security']['is_cloud_provider']:
                    TYPE['hosting'] = True   
                
                elif rJSON['company']['type'] == "isp":
                    if rJSON['connection']['type'] == "isp":
                        TYPE['residential'] = True
                    elif rJSON['connection']['type'] == "business":
                        TYPE['business'] = True
                    else:
                        TYPE['hosting'] = True
                        
                elif rJSON['company']['type'] == "business":    
                    if rJSON['connection']['type'] == "hosting":
                        TYPE['hosting'] = True
                    else:
                        TYPE['business'] = True
                            
                elif rJSON['company']['type'] == "education":
                    TYPE['education'] = True
                
                elif rJSON['company']['type'] == "government":
                    TYPE['government'] = True
                    
                for k,v in TYPE.items():
                    if v:
                        self.UpdateNodeTypeTable(db, node,k)
            except KeyError:
                pass
            sleep(1)
            
    def UpdateNodeTypeTable(self, db, node, type):
        
        query = 'UPDATE node_score SET isp_type = "%s" WHERE node_address = "%s";' % (type, node)
        print(query)
        c = db.cursor();
        c.execute(query)
        db.commit()
            
if __name__ == "__main__":
    NType = UpdateNodeType()
    db = NType.connDB()
    NodeData = NType.get_node_type_table(db)
    NodeIP = NType.get_ip_of_node(db, NodeData)
    NType.query_ipregistry_co(db,NodeIP)
    
                    
            


