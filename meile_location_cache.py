from subprocess import Popen, PIPE, STDOUT
import pymysql
import scrtsxx
import time
from urllib3.exceptions import InsecureRequestWarning
import requests
import warnings

NodesInfoKeys = ["Moniker","Address","Provider","Price","Country","Speed","Latency","Peers","Handshake","Version","Status"]
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
            #print(line)
            if k < 4:  
                k += 1 
                continue
            if k >=4 and '+-------+' in str(line.decode('utf-8')):
                break
            elif "freak12techno" in str(line.decode('utf-8')):
                ninfos = []
                ninfos.append(str(line.decode('utf-8')).split('|')[1])
                for ninf in str(line.decode('utf-8')).split('|')[3:-1]:
                    ninfos.append(ninf)
                AllNodesInfo.append(dict(zip(NodesInfoKeys, ninfos)))
            elif "Testserver" in str(line.decode('utf-8')):
                continue
            else: 
                ninfos = str(line.decode('utf-8')).split('|')[1:-1]
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


        for n in nodes:
            endpoint = "/nodes/" + n
            #print(APIURL + endpoint)
            try:
                requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
                r = requests.get(APIURL + endpoint)
                remote_url = r.json()['result']['node']['remote_url']
                r = requests.get(remote_url + "/status", verify=False)
        
                NodeInfoJSON       = r.json() 
                NodeLoc[n] = NodeInfoJSON['result']['location']['city']
            except Exception as e:
                print(str(e))
                
        return NodeLoc
            
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
            
    
    
if __name__ == "__main__":
    start = time.time()
    NodeLoc = UpdateNodeLocations()
    db = NodeLoc.connDB()
    Nodes = NodeLoc.get_nodes("15s")
    elapsed = (time.time() - start)
    print("Node data took: %.3fs" % elapsed)
    start = time.time()
    Locations = NodeLoc.get_city_of_node(Nodes)
    #print(Locations)
    elapsed = (time.time() - start)
    print("Retrieving cities took: %.3fs" % elapsed)
    start = time.time()
    NodeLoc.UpdateNodeLocDB(db, Locations)
    elapsed = (time.time() - start)
    print("Updating DB took: %.3fs" % elapsed)

   