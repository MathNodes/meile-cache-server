import random
import json
import pymysql
import scrtsxx
from sentinel_sdk.sdk import SDKInstance
from sentinel_sdk.types import PageRequest, Status
from sentinel_sdk.modules.node import NodeModule


GRPC_HOST = "aimokoivunen.mathnodes.com"
GRPC_PORT = 9090
TIMEOUT = 30
VERSION = 20240202.1954

class OnlineNodes():
    
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
    
    def DropTableAndCreateNew(self, db):
        
        drop_query = "DROP TABLE online_nodes;"
        c = db.cursor()
        
        c.execute(drop_query)
        db.commit()
        
        create_query = '''
        CREATE TABLE online_nodes (node_address VARCHAR(100) NOT NULL, moniker VARCHAR(500), country VARCHAR(100), city VARCHAR(100),
        latitude DECIMAL(7,4), longitude DECIMAL(7,4), gigabyte_prices VARCHAR(2000), hourly_prices VARCHAR(2000), 
        bandwidth_down BIGINT UNSIGNED, bandwidth_up BIGINT UNSIGNED, wallet VARCHAR(100), handshake BOOLEAN, connected_peers SMALLINT UNSIGNED,
        max_peers SMALLINT UNSIGNED, node_type TINYINT UNSIGNED, node_version VARCHAR(20), PRIMARY KEY(node_address));
        '''
        
        c.execute(create_query)
        db.commit()
        
    def InsertRow(self, db, q):
        
        c= db.cursor()
        c.execute(q)
        db.commit()
        
    
    def QueryAndRepopulateDB(self, db):
        sdk = SDKInstance("aimokoivunen.mathnodes.com", 9090)
        nm = NodeModule(sdk._channel,TIMEOUT,sdk._account,sdk._client)
        
        nodes = nm.QueryNodes(status=Status.ACTIVE, pagination=PageRequest(limit=1000))
        
        nodesStatus = sdk.nodes.QueryNodesStatus(nodes)
        
        '''value of nodesStatus
        ('sentnode16gswagztkv4q8h4hc89stk2ndc2avgthc45lpx', '{"success":true,"result":{"address":"sentnode16gswagztkv4q8h4hc89stk2ndc2avgthc45lpx","bandwidth":{"download":166750000,"upload":321125000},"handshake":{"enable":false,"peers":8},"interval_set_sessions":10000000000,"interval_update_sessions":6900000000000,"interval_update_status":3300000000000,
        "location":{"city":"Bangkok","country":"Thailand","latitude":13.8054,"longitude":100.6751},"moniker":"Mrsilent-317","operator":"sent16gswagztkv4q8h4hc89stk2ndc2avgthwr4xys",
        "peers":0,"gigabyte_prices":"52573ibc/31FEE1A2A9F9C01113F90BD0BBCCE8FD6BBB8585FAF109A2101827DD1D5B95B8,9204ibc/A8C2D23A1E6F95DA4E48BA349667E322BD7A6C996D8A4AAE8BA72E190F3D1477,1180852ibc/B1C0DDB14F25279A2026BC8794E12B259F8BDA546A3C5132CCAEE4431CE36783,122740ibc/ED07A3391A112B175915CD8FAF43A2DA8E4790EDE12566649D0C2F97716B8518,15342624udvpn",
        "hourly_prices":"18480ibc/31FEE1A2A9F9C01113F90BD0BBCCE8FD6BBB8585FAF109A2101827DD1D5B95B8,770ibc/A8C2D23A1E6F95DA4E48BA349667E322BD7A6C996D8A4AAE8BA72E190F3D1477,1871892ibc/B1C0DDB14F25279A2026BC8794E12B259F8BDA546A3C5132CCAEE4431CE36783,18897ibc/ED07A3391A112B175915CD8FAF43A2DA8E4790EDE12566649D0C2F97716B8518,4160000udvpn",
        "qos":{"max_peers":250},"type":2,"version":"0.7.1"}}')
        '''
        for a,d in nodesStatus.items():
            if json.loads(d)['success']:
                result = json.loads(d)['result']
                address        = result['address'] # VARCHAR(100)
                bandwidth_down = int(result['bandwidth']['download']) # BIGINT
                bandwidth_up   = int(result['bandwidth']['upload']) # BIGINT
                handshake      = result['handshake']['enable'] # Boolean
                city           = result['location']['city'] # VARCHAR(100)
                country        = result['location']['country'] #VARCHAR(100)
                latitude       = float(result['location']['latitude']) #DECIMAL(7,4)
                longitude      = float(result['location']['longitude']) #DECIMAL(7,4)
                moniker        = result['moniker'] # VARCHAR(200)
                wallet         = result['operator'] #VARCHAR(100)
                peers          = int(result['peers']) #SMALLINT
                gb_prices      = result['gigabyte_prices'] #VARCHAR(2000)
                hr_prices      = result['hourly_prices'] #VARCHAR(2000)
                max_peers      = int(result['qos']['max_peers']) #SMALLINT
                node_type      = int(result['type']) #SMALLINT
                node_version   = result['version'] #VARCHAR(20)
                
                iquery = '''
                INSERT IGNORE INTO online_nodes (node_address, moniker, country, city, latitude, longitude, gigabyte_prices, hourly_prices, bandwidth_down, bandwidth_up, wallet, handshake, connected_peers, max_peers, node_type, node_version) 
                VALUES ("%s", "%s", "%s", "%s", %.4f, %.4f, "%s", "%s", %d, %d, "%s", "%s", %d, %d, %d, "%s")
                ''' % (address,
                       moniker, 
                       country, 
                       city, 
                       latitude, 
                       longitude, 
                       gb_prices, 
                       hr_prices, 
                       bandwidth_down, 
                       bandwidth_up, 
                       wallet, 
                       handshake, 
                       peers, 
                       max_peers,
                       node_type, 
                       node_version)  
                self.InsertRow(db, iquery)
        
if __name__ == "__main__":
    on = OnlineNodes()
    db = on.connDB()
    on.DropTableAndCreateNew(db)
    on.QueryAndRepopulateDB(db)
    
