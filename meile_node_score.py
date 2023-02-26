from subprocess import Popen, PIPE, STDOUT
import pymysql
import scrtsxx
import time
from urllib3.exceptions import InsecureRequestWarning
import requests
import warnings
import ipaddress
import socket


API = 'https://api.sentinel.mathnodes.com'
API_KEY = scrtsxx.IP_DATA_API_KEY



class UpdateNodeScoreAPI():
    
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
    
    def appendNodeScoreTable(self,db):
        iquery = "INSERT IGNORE INTO node_score (node_address) SELECT node_address FROM node_cities;"
        
        c = db.cursor()
        c.execute(iquery)
        db.commit()
        
    
    def getNodeScoreTable(self,db):
        query = "SELECT * FROM node_score"
        
        c = db.cursor()
        
        c.execute(query)
        return c.fetchall()
    
    
    def parseNodeScoreTable(self, db, ns_table):
        
        remainingNodeScores = []
        
        for n in ns_table: 
            if not n['asn']:
                remainingNodeScores.append(n)
                
        return remainingNodeScores
    
    def getNodeURL(self, db, ns_table):
        NodeURLs = []
        NodeDict = {'node_address' : None, 'ip' : None}
        
        for n in ns_table:
            endpoint = API + '/nodes/' + n['node_address']
            
            try:
                r = requests.get(endpoint)
                ip = r.json()['result']['node']['remote_url'].split('//')[-1].split(':')[0]
                NodeDict['node_address'] = n['node_address']
                NodeDict['ip'] = ip
                NodeURLs.append(NodeDict.copy())
                print(NodeDict)
            except:
                pass
            time.sleep(1)
        
        return NodeURLs
    def getIPAddress(self, nodes):
        NodeURLs = []
        NodeDict = {'node_address' : None, 'ip' : None}
        print(nodes)
        for n in nodes:
            print(n)
            NodeDict['node_address'] = n['node_address']
            try: 
                NodeDict['ip'] = ipaddress.ip_address(n['ip'])
            except ValueError:
                NodeDict['ip'] = socket.gethostbyname(n['ip'])
            print(NodeDict['ip'])
            NodeURLs.append(NodeDict.copy())
        #print(NodeURLs)    
        return NodeURLs
            
    def getNodeScores(self, db, nodes):
        NodeScoreTable = []
        NodeScoreInfo = {'node_address' : None,
                         'asn' : None,
                         'isp' : None,
                         'isp_type' : None,
                         'datacenter' : True }
        c = db.cursor()
        nodes = self.getIPAddress(nodes)
        print(nodes)
        #enter = input("Press Enter: ")
        
        for n in nodes:
            IPDATAURL = "https://api.ipdata.co/%s?api-key=%s" % (n['ip'], API_KEY)
            try:
                r = requests.get(IPDATAURL)
                print(r.json())
                NodeScoreInfo['node_address'] = n['node_address']
                NodeScoreInfo['asn'] = r.json()['asn']['asn']
                NodeScoreInfo['isp'] = r.json()['asn']['name']
                NodeScoreInfo['isp_type'] = r.json()['asn']['type']
                NodeScoreInfo['datacenter'] = r.json()['threat']['is_datacenter']
                print(NodeScoreInfo)
                NodeScoreTable.append(NodeScoreInfo.copy())
            except Exception as e:
                print(str(e))
                continue
            time.sleep(5)
            #wnrwe = input("Press Emter: ")
        
        for n in NodeScoreTable:
            try: 
                query = 'UPDATE node_score SET  asn = "%s", isp = "%s", isp_type = "%s", datacenter = %s WHERE node_address = "%s";' % (n['asn'],
                                                                                                                                        n['isp'],
                                                                                                                                        n['isp_type'],
                                                                                                                                        n['datacenter'],
                                                                                                                                        n['node_address'])
                
                print(query)
                c.execute(query)
                db.commit()
            except Exception as e:
                print(str(e))
                continue
        
            
        
            
                        
                
        
        
        
if __name__ == "__main__":
    NodeScores = UpdateNodeScoreAPI()
    db = NodeScores.connDB()
    NodeScores.appendNodeScoreTable(db)
    table = NodeScores.getNodeScoreTable(db)
    new_table = NodeScores.parseNodeScoreTable(db, table)
    ips = NodeScores.getNodeURL(db, new_table)
    print(ips)
    #enter = input("Press Enter: ")
    NodeScores.getNodeScores(db, ips)
    
    
    
    