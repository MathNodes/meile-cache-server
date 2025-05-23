#!/bin/env python3

import requests
import random
import scrtsxx
import socket
import pymysql
import ipaddress
from time import sleep 
import concurrent.futures
from dbutils.pooled_db import PooledDB


APIKEYS = scrtsxx.IP_REGISTRY_API_KEYS

VERSION = 20250319.1453
APIURL = 'https://api.sentinel.mathnodes.com'

IPREGISTRY_URL = "https://api.ipregistry.co/%s?key=%s"

class UpdateNodeType():
    NodeAPIurl = {}
    
    def connDB(self): 
        
        self.db_pool = PooledDB(
            creator=pymysql, 
            maxconnections=5, 
            host=scrtsxx.HOST, 
            port=scrtsxx.PORT,
            user=scrtsxx.USERNAME,
            passwd=scrtsxx.PASSWORD,
            database=scrtsxx.DB,      
        )
        
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
        NodeDBIP = {}
        NodeIP = {}
        NodeIPURLChanged = {}
        
        c = db.cursor()
        
        self.api_rurl_multithread(NodeData)

        for n in NodeData:
            address = n['node_address']
            

            # Retrieve remote_url from the table for nodes that have it stored
            query = f"SELECT remote_url FROM node_uptime WHERE node_address = '{address}';"
            query2 = f"SELECT exitIp FROM exitip WHERE addr = '{address}';"
            c.execute(query)
            result = c.fetchone()
            c.execute(query2)
            exitresult = c.fetchone()
            #print(result['remote_url'])
            try:
                db_rurl = result['remote_url']
            except:
                db_rurl = ""
                
            NodeDBIP[address] = db_rurl
            try:         
                if NodeDBIP[address] != self.NodeAPIurl[address]:
                    self.__UpdateUptimeTable(db, address, self.NodeAPIurl[address])
                    remote_url = self.NodeAPIurl[address].split('//')[-1].split(':')[0]
                    try:
                        rurlip = ipaddress.ip_address(remote_url)
                        if exitresult['exitIp']:
                            exitIp = ipaddress.ip_address(exitresult['exitIp'])
                            if rurlip != exitIp: 
                                print(f"{rurlip},{exitIp}")
                                NodeIPURLChanged[address] = exitIp
                            else:
                                NodeIPURLChanged[address] = rurlip 
                        else:
                            NodeIPURLChanged[address] = rurlip 
                        
                    except ValueError:
                        try:
                            rurlip = socket.gethostbyname(remote_url)
                            if exitresult['exitIp']:
                                exitIp = ipaddress.ip_address(exitresult['exitIp'])
                                if rurlip != exitIp: 
                                    print(f"{rurlip},{exitIp}")
                                    NodeIPURLChanged[address] = exitIp
                                else:
                                    NodeIPURLChanged[address] = rurlip 
                            else:
                                NodeIPURLChanged[address] = rurlip 
                        except socket.gaierror:
                            continue
                        
                else:
                    remote_url = NodeDBIP[address].split('//')[-1].split(':')[0]
                    try:  
                        rurlip = ipaddress.ip_address(remote_url)
                        if exitresult['exitIp']:
                            exitIp = ipaddress.ip_address(exitresult['exitIp'])
                            if rurlip != exitIp:
                                print(f"{rurlip},{exitIp}")
                                NodeIP[address] = exitIp
                            else:
                                NodeIP[address] = rurlip 
                        else:
                            NodeIP[address] = rurlip 
                    except ValueError:
                        try:
                            rurlip = socket.gethostbyname(remote_url)
                            if exitresult['exitIp']:
                                exitIp = ipaddress.ip_address(exitresult['exitIp'])
                                if rurlip != exitIp:
                                    print(f"{rurlip},{exitIp}")
                                    NodeIP[address] = exitIp
                                else:
                                    NodeIP[address] = rurlip
                            else:
                                NodeIP[address] = rurlip
                        except socket.gaierror:
                            continue
            except Exception as e:
                print(f"{n}:{str(e)}")
                continue
            
        return NodeIP, NodeIPURLChanged


    def check_asn_null(self, db, node_address):
        connection = self.db_pool.connection()
        cursor     = connection.cursor()
        
        query = f"SELECT asn FROM node_score WHERE node_address = '{node_address}';"
        
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            print(result)
        finally:
            cursor.close()
            connection.close()
            
        if result and result[0] is None:
            return True
        print("RETURNING FALSE")
        return False
    
    def api_rurl_multithread(self, NodeData):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Submit tasks in batches of 3
            futures = [executor.submit(self.__api_url_worker, node['node_address']) for node in NodeData]

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # This will raise any exceptions that occurred in the thread
                except Exception as e:
                    print("An error occurred:", str(e))
        
    def ip_registry_multithread(self, db, NodeIP, changed):
        #print(NodeIP)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Submit tasks in batches of 3
            futures = [executor.submit(self.__ip_registry_worker, node, ip, db, changed) for node, ip in NodeIP.items()]

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # This will raise any exceptions that occurred in the thread
                except Exception as e:
                    print("An error occurred:", str(e))
            
    def __api_url_worker(self, address):
        endpoint = APIURL + '/sentinel/nodes/' + address
        
        try: 
            r = requests.get(endpoint)
            api_rurl = r.json()['node']['remote_url']
        except Exception as e:
            print(f"API URL WORKER ERROR: {str(e)}")
            api_rurl = ""
            
        self.NodeAPIurl[address] = api_rurl
        
        
    def __ip_registry_worker(self, node, ip, db, changed):
        if changed:
            print("CHANGED REMOTE URLs... CHECKING....")
            self.__check_nodes(node, ip, db)
        
        if not changed and self.check_asn_null(db, node):
            print("NOT CHANGED - CHECK NULL NODE")
            self.__check_nodes(node, ip, db)
                

        
    def __check_nodes(self, node, ip, db):
        N = random.randint(0,len(APIKEYS)-1)
        API_KEY = APIKEYS[N]
        TYPE = {"residential" : False, "business" : False, "hosting" : False, "education" : False, "government" : False }
        try: 
            resp = requests.get(IPREGISTRY_URL % (ip,API_KEY))
            rJSON = resp.json()
            sleep(.3)
        except Exception as e:
            print(str(e))
            return 
        try:
            ASN = "AS" + str(rJSON['connection']['asn'])
            ISP = rJSON['connection']['organization']
            
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
                    self.__UpdateNodeTypeTable(db, node,k, ASN, ISP)
        except KeyError as e:
            print(str(e))
            pass    
        
        
    def __UpdateNodeTypeTable(self, db, node, ntype, asn, isp):
        connection = self.db_pool.connection()
        cursor     = connection.cursor()
        
        query = 'UPDATE node_score SET asn = "%s", isp = "%s", isp_type = "%s" WHERE node_address = "%s";' % (asn, isp, ntype, node)
        print(query)
        try: 
            cursor.execute(query)
            connection.commit()
        finally:
            cursor.close()
            connection.close()
        
        
    def __UpdateUptimeTable(self, db, node, rurl):
        query = 'UPDATE node_uptime SET remote_url = "%s" WHERE node_address = "%s";' % (rurl, node)
        print(f"Updating node_uptime:\n {query}")
        
        c = db.cursor()
        c.execute(query)
        db.commit()
            
if __name__ == "__main__":
    NType = UpdateNodeType()
    db = NType.connDB()
    NodeData = NType.get_node_type_table(db)
    NodeIP, URLsChanged = NType.get_ip_of_node(db, NodeData)
    NType.ip_registry_multithread(db, NodeIP, False)
    print("------------------Computing URLs Changed---------------------------")
    NType.ip_registry_multithread(db, URLsChanged, True)
    
                    
            


