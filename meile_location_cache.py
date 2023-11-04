#!/bin/env python3

from subprocess import Popen, PIPE, STDOUT
import pymysql
import scrtsxx
import time
from urllib3.exceptions import InsecureRequestWarning
import requests
import warnings

from time import sleep

VERSION = 202311040305.37

NodesInfoKeys = ["Moniker","Address","Price","Hourly Price", "Country","Speed","Latency","Peers","Handshake","Type","Version","Status"]
APIURL        = "https://api.sentinel.mathnodes.com"
class UpdateNodeLocations():
    sentinelcli = "/home/sentinel/sentinelcli"
    RPC = "https://rpc.mathnodes.com:443"
    def get_nodes(self, latency, *kwargs):
        AllNodesInfo = []
        print("Running sentinel-cli with latency: %s" % latency)
        nodeCMD = [self.sentinelcli, "query", "nodes", "--node", self.RPC, "--limit", "20000", "--timeout", "%s" % latency]
    
        proc = Popen(nodeCMD, stdout=PIPE)
        
        k=1
        
        
        for line in proc.stdout.readlines():
            line = str(line.decode('utf-8'))
            if k < 4:  
                k += 1 
                continue
            if k >=4 and '+-------+' in str(line):
                break
            elif "freak12techno" in str(line):
                ninfos = []
                ninfos.append(str(line).split('|')[1])
                for ninf in str(line).split('|')[3:-1]:
                    ninfos.append(ninf)
                AllNodesInfo.append(dict(zip(NodesInfoKeys, ninfos)))
            elif "Testserver" in str(line):
                continue
            else: 
                ninfos = str(line).split('|')[1:-1]
                if ninfos[0].isspace():
                    continue
                elif ninfos[1].isspace():
                    continue
                else:
                    AllNodesInfo.append(dict(zip(NodesInfoKeys, ninfos)))
                #print(ninfos, end='\n')
        
        #get = input("Blah: ")
        AllNodesInfoSorted = sorted(AllNodesInfo, key=lambda d: d[NodesInfoKeys[4]])
        
        NodeAddresses = []
        for node in AllNodesInfoSorted:
            NodeAddresses.append(node['Address'].lstrip().rstrip())
            
        return NodeAddresses
            
    
    def get_city_of_node(self, nodes):   
        
        NodeLoc = {}
        NodeURL = {}

        for n in nodes:
            endpoint = "/sentinel/nodes/" + n
            #print(APIURL + endpoint)
            try:
                requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
                r = requests.get(APIURL + endpoint)
                remote_url = r.json()['node']['remote_url']
                print(f"{remote_url}", end=' ')
                r = requests.get(remote_url + "/status", verify=False)
                print(r.status_code)
                NodeInfoJSON  = r.json()
                print(NodeInfoJSON) 
                NodeLoc[n]    = NodeInfoJSON['result']['location']['city']
                
                NodeURL[n]    = remote_url
            except Exception as e:
                print(str(e))
                
        return NodeLoc, NodeURL
            
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
    
    def UpdateNodeLocDB(self, db, node_locations):
        c = db.cursor()
        
        for n in node_locations.keys():
            query = 'REPLACE INTO node_cities (node_address, city) VALUES ("%s", "%s");' % (n, node_locations[n])
            with warnings.catch_warnings():
                warnings.simplefilter("ignore") 
                c.execute(query)
                db.commit()
                
    def UpdateRemoteURLsInUptimeTable(self, db, NodeURLs):
        c = db.cursor()
        for k,v in NodeURLs.items():
            q = '''
                INSERT INTO node_uptime (node_address, remote_url, tries, success, success_rate)
                VALUES ("%s","%s",0,0,0.0)
                ON DUPLICATE KEY UPDATE remote_url = "%s";
                ''' % (k,v,v)
            print(q)
            sleep(1)     
            with warnings.catch_warnings():
                warnings.simplefilter("ignore") 
                c.execute(q)
                db.commit()
                     
    
    
if __name__ == "__main__":
    start = time.time()
    print("meile_location_cache.py - v%s" % VERSION)
    NodeLoc = UpdateNodeLocations()
    db = NodeLoc.connDB()
    Nodes = NodeLoc.get_nodes("20s")
    elapsed = (time.time() - start)
    print("Node data took: %.3fs, there are %d nodes" % (elapsed, len(Nodes)))
    start = time.time()
    Locations, URLs = NodeLoc.get_city_of_node(Nodes)
    elapsed = (time.time() - start)
    print("Retrieving cities took: %.3fs" % elapsed)
    start = time.time()
    NodeLoc.UpdateRemoteURLsInUptimeTable(db, URLs)
    elapsed = (time.time() - start)
    print("Updating node_uptime table took: %.3fs" % elapsed)
    start = time.time()
    NodeLoc.UpdateNodeLocDB(db, Locations)
    elapsed = (time.time() - start)
    print("Updating DB took: %.3fs" % elapsed)

   