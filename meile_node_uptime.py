#!/bin/env python3

import pymysql
import scrtsxx
import requests
import socket
import sys
from contextlib import closing
from timeit import default_timer as timer
from urllib.parse import urlparse
import concurrent.futures
from dbutils.pooled_db import PooledDB

VERSION = 20250304.1738
WORKERS = 50 # edit. This should be plenty.
APIURL = 'https://api.sentinel.mathnodes.com'

class UpdateNodeUptime():
    
    def connDB(self): 
        
        self.db_pool = PooledDB(
            creator=pymysql, 
            maxconnections=50, 
            host=scrtsxx.RHOST, 
            port=scrtsxx.PORT,
            user=scrtsxx.USERNAME,
            passwd=scrtsxx.PASSWORD,
            database=scrtsxx.DB,      
        )
        
        self.local_db_pool = PooledDB(
            creator=pymysql, 
            maxconnections=50, 
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
    
    def get_node_uptime_table(self,db):
        
        query = "SELECT * FROM node_uptime;"
        c = db.cursor()
        c.execute(query)
        
        return c.fetchall()
    
    
    def get_remote_url_of_node(self, db, NodeData):
        NodeRemoteURL = {'address' : [], 'url' : []}
        
        def get_remote_url(n):
            address = n['node_address']
            #print(address)
            query = f"SELECT remote_url FROM node_uptime WHERE node_address = '{address}';"
            connection = self.local_db_pool.connection()
            #c = db.cursor()
            c = connection.cursor()
            c.execute(query)
            result = c.fetchone()
            #print(result)
            connection.close()
            if not result[0]:
                endpoint = APIURL + '/sentinel/nodes/' + address
                remote_url = ''
                print(f"Getting remote_url of: {address}")
                sys.stdout.flush()
                
                try:
                    r = requests.get(endpoint)
                    remote_url = r.json()['node']['remote_url']
                except Exception as e:
                    print(str(e))
                    return n['node_address'], None
            else:
                remote_url = result[0]
            
            return n['node_address'], remote_url
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(get_remote_url, n) for n in NodeData]
            results = [future.result() for future in futures]
        
        for address, remote_url in results:
            NodeRemoteURL['address'].append(address)
            NodeRemoteURL['url'].append(remote_url)
        
        return NodeRemoteURL
    
    def check_uptime(self, NodeRemoteURLs):
        k = 0
        NodeUptimeBoolean = {'address': [], 'up': []}
        
        def check_uptime_for_node(n, url):
            parsed_url = urlparse(url)
            netloc = parsed_url.netloc
            
            if '[' in netloc and ']' in netloc:
                host = netloc.split(']', 1)[0][1:]
                if ':' in netloc.split(']', 1)[1]:
                    port = int(netloc.split(']', 1)[1].split(':', 1)[1])
                else:
                    port = None
            else:
                parts = netloc.split(':')
                if len(parts) > 2:
                    raise ValueError("Invalid URL format")
                host = parts[0]
                if len(parts) == 2:
                    port = int(parts[1])
                else:
                    port = None
            
            up = self.check_socket(host, int(port) if port else None)
            result = (n, up)
            #print(result)
            return result
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = []
            
            for n in NodeRemoteURLs['address']:
                url = NodeRemoteURLs['url'][k]
                futures.append(executor.submit(check_uptime_for_node, n, url))
                k += 1
            
            for future in concurrent.futures.as_completed(futures):
                address, up = future.result()
                NodeUptimeBoolean['address'].append(address)
                NodeUptimeBoolean['up'].append(up)
    
        return NodeUptimeBoolean

    def check_socket(self, host, port):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(3)
            try: 
                if sock.connect_ex((host, port)) == 0:
                    return True
                else:
                    return False
            except socket.gaierror as e:
                print(f"Socket Err: {host}:{port}")
                print(str(e))
                return False
            
            
    def UpdateNodeUptimeTable(self, db, NodeUptimeData, NodeRemoteURLs, NodeUptimeBoolean):
        
        query = 'UPDATE node_uptime SET remote_url = "%s", tries = %s, success = %s, success_rate = "%s" WHERE node_address = "%s";'
        update_data = []
        for node in NodeUptimeData:
            try: 
                index  = NodeUptimeBoolean['address'].index(node['node_address'])
                rindex = NodeRemoteURLs['address'].index(node['node_address'])
                remote_url = NodeRemoteURLs['url'][rindex]    
            except Exception as e:
                print(node)
                print(str(e))
                return
            
            tries = node['tries'] + 1
            if NodeUptimeBoolean['up'][index]:
                success = node['success'] + 1                
            else:
                success = node['success']
            success_rate = round(float(success/tries),3)
            
            update_data.append((remote_url,
                               tries,
                               success,
                               success_rate,
                               node['node_address']))

            if len(update_data) == 200:
                # Use if not running from a remote server and comment the line after the next one. 
                #connection = self.local_db_pool.connection() 
                connection = self.db_pool.connection()
                cursor     = connection.cursor()
                cursor.executemany(query, update_data)
                connection.commit()
                connection.close()
                update_data.clear()
                        
if __name__ == "__main__":
    NodeUptimeClass = UpdateNodeUptime()
    db = NodeUptimeClass.connDB()
    
    start = timer()
    NodeUptimeData = NodeUptimeClass.get_node_uptime_table(db)
    end = timer()
    
    time1 = round((end - start),4)
    print("It took %ss to get node uptime table" % (time1))
    
    start = timer()
    NodeRemoteURLs = NodeUptimeClass.get_remote_url_of_node(db, NodeUptimeData)
    end = timer()
    
    time2 = round((end - start),4)
    print("It took %ss to get node remote URLs" % (time2))
    
    start = timer()
    NodeUptimeBoolean = NodeUptimeClass.check_uptime(NodeRemoteURLs)
    end = timer()
    
    time3 = round((end - start),4)
    print("It took %ss to check if all nodes are connectable" % (time3))
    
    start = timer()
    NodeUptimeClass.UpdateNodeUptimeTable(db, NodeUptimeData, NodeRemoteURLs, NodeUptimeBoolean)
    end = timer()
    
    time4 = round((end - start),4)
    print("It took %ss to update the node_uptime table" % (time4))
    
    total_time = time1 + time2 + time3 + time4
    print("Total Elapsed time: %ss" % total_time)
    
    
    
    
    
