import pymysql
import scrtsxx
import requests
from timeit import default_timer as timer
from urllib3.exceptions import InsecureRequestWarning
import urllib3

APIURL = 'https://api.sentinel.mathnodes.com'

class UpdateNodeGeoIP():
    
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
    
    def get_geoIP_table(self, db):
        
        query = "SELECT * from node_geoip;"
        c = db.cursor()
        c.execute(query)
        
        return c.fetchall()
    
    def get_remote_url_of_node(self, NodeData):
        NodeRemoteURL = {'address' : [], 'url' : []}
        
        for n in NodeData:
            address = n['node_address']
            endpoint = APIURL + '/nodes/' + address
            
            try: 
                r = requests.get(endpoint)
                remote_url = r.json()['result']['node']['remote_url']
                NodeRemoteURL['address'].append(n['node_address'])
                NodeRemoteURL['url'].append(remote_url)
            except Exception as e:
                print(str(e))
                continue
        
        #print(NodeRemoteURL)
        
        return NodeRemoteURL
    
    def get_node_geoIP(self, NodeData, NodeRemoteURLs):
        
        
        queries = []
        
        for node in NodeData:
            if not node['latitude'] or not node['longitude']:
                try:
                    rindex = NodeRemoteURLs['address'].index(node['node_address'])
                except Exception as e:
                    continue
                    
                NodeLoc = self.get_city_of_node(NodeRemoteURLs['url'][rindex])
                
                query = 'UPDATE node_geoip SET moniker = "%s", country = "%s", city = "%s", latitude = "%s", longitude = "%s" WHERE node_address = "%s";' % (NodeLoc['moniker'],
                                                                                                                                                            NodeLoc['country'],
                                                                                                                                                            NodeLoc['city'],
                                                                                                                                                            NodeLoc['latitude'],
                                                                                                                                                            NodeLoc['longitude'],
                                                                                                                                                            node['node_address'])
                queries.append(query)
                
        return queries
                
    def get_city_of_node(self, remote_url):   
        
        NodeLoc = {'city' : None, 'country' : None, 'latitude' : None, 'longitude' : None, 'moniker' : None}

        try:
            requests.packages.urllib3.disable_warnings()
            #requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
            r = requests.get(remote_url + "/status", verify=False, timeout=6)
    
            NodeInfoJSON  = r.json() 
            NodeLoc['city'] = NodeInfoJSON['result']['location']['city']
            NodeLoc['country'] = NodeInfoJSON['result']['location']['country']
            NodeLoc['latitude'] = NodeInfoJSON['result']['location']['latitude']
            NodeLoc['longitude'] = NodeInfoJSON['result']['location']['longitude']
            NodeLoc['moniker'] = NodeInfoJSON['result']['moniker']
        except Exception as e: 
            #print(str(e))
            pass
                
        return NodeLoc
        
        
    def UpdateGeoIPNodeTable(self, db, queries):
        
        c = db.cursor()
        
        for q in queries:
            print(q)
            c.execute(q)
            db.commit()
            
            
if __name__ == "__main__":
    NodeGeoIP = UpdateNodeGeoIP()
    db = NodeGeoIP.connDB()
    
    start = timer()
    table = NodeGeoIP.get_geoIP_table(db)
    end = timer()
    
    time1 = round((end - start),3)
    print("It took %ss to get node GeoIP table" % time1)
    
    start = timer()
    NodeRemoteURLs = NodeGeoIP.get_remote_url_of_node(table)
    end = timer()
    
    time2 = round((end-start), 3)
    print("It took %ss to get node remote_urls" % time2)
    
    start = timer()
    q = NodeGeoIP.get_node_geoIP(table, NodeRemoteURLs)
    end = timer()
    
    time3 = round((end-start),3)
    print("It took %ss to populate GeoIP queries" % time3)
    
    start = timer()
    NodeGeoIP.UpdateGeoIPNodeTable(db, q)
    end = timer()
    
    time4 = round((end-start), 3)
    print("It took %ss to update node_geoip table" % time4)
    
    TimeElapased = time1 + time2 + time3 + time4
    print("Total Elapsed Time: %ss" % TimeElapased)
    