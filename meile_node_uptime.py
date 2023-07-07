import pymysql
import scrtsxx
import requests
import socket
from contextlib import closing
from timeit import default_timer as timer


APIURL = 'https://api.sentinel.mathnodes.com'

class UpdateNodeUptime():
    
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
    
    def get_node_uptime_table(self,db):
        
        query = "SELECT * FROM node_uptime;"
        c = db.cursor()
        c.execute(query)
        
        return c.fetchall()
    
    
    def get_remote_url_of_node(self, db, NodeData):
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

   
    def check_uptime(self, NodeRemoteURLs):
        k=0
        
        NodeUptimeBoolean = {'address' : [], 'up' : []}
        
        
        for n in NodeRemoteURLs['address']:
            url = NodeRemoteURLs['url'][k]

            hp = url.split('//')[-1]
            host, port = hp.split(":")
            #print(f"host: {host}, port: {port}")
            #print("Checking if up: ", end='')
            
            up = self.check_socket(host, int(port.replace('/','')))
            #print(up)
            NodeUptimeBoolean['address'].append(n)
            NodeUptimeBoolean['up'].append(up)
            k += 1
            
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
                print(str(e))
                return False
            
            
    def UpdateNodeUptimeTable(self, db, NodeUptimeData, NodeRemoteURLs, NodeUptimeBoolean):
        for node in NodeUptimeData:
            
            try: 
                index  = NodeUptimeBoolean['address'].index(node['node_address'])
                rindex = NodeRemoteURLs['address'].index(node['node_address'])
                remote_url = NodeRemoteURLs['url'][rindex]    
            except Exception as e:
                print(str(e))
                continue
            
            tries = node['tries'] + 1
            if NodeUptimeBoolean['up'][index]:
                success = node['success'] + 1                
            else:
                success = node['success']
            success_rate = round(float(success/tries),3)
            
            query = 'UPDATE node_uptime SET remote_url = "%s", tries = %d, success = %d, success_rate = "%.3f" WHERE node_address = "%s";' % (remote_url,
                                                                                                                                              tries,
                                                                                                                                              success,
                                                                                                                                              success_rate,
                                                                                                                                              node['node_address'])
            
            #print(query)
            c = db.cursor()
            c.execute(query)
            db.commit()    
            
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
    print("Total Elapsed time: %s" % total_time)
    
    
    
    
    
