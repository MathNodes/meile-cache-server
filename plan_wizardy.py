#!/bin/env python3

import pymysql
import scrtsxx
import requests

from timeit import default_timer as timer
from datetime import datetime


VERSION = 20240716.0236

class PlanWizard():
    
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
    
    
    def getPlanWizardData(self):
        base_url = "https://planwizard.basedapps.co.uk/plans/{}/nodes?limit=100000&offset=0"
        
        data_dict = {}
        
        for plan in range(1, 4):
            url = base_url.format(plan)
            response = requests.get(url)
            print(f"Getting Plan: {plan}")
            if response.status_code == 200:
                data = response.json()
                data_dict[plan] = data
            else:
                print(f"Failed to fetch data for plan {plan}. Status code: {response.status_code}")
                
        return data_dict
    
    def insertPlanWizardData(self, db, data_dict):
        c = db.cursor()
        
        # Insert data into the MySQL table
        insert_query = """
        INSERT IGNORE INTO plan_wizard (
            id, created_at, is_active, revision, is_node_status_fetched, last_node_status_fetch,
            is_network_info_fetched, last_network_info_fetch, is_health_checked, last_health_check,
            is_whitelist_info_fetched, last_whitelist_info_fetch, address, remote_url, status, 
            status_at, inactive_at, moniker, is_residential, is_healthy, is_whitelisted, plan_id
        ) VALUES (
            %(id)s, %(created_at)s, %(is_active)s, %(revision)s, %(is_node_status_fetched)s, %(last_node_status_fetch)s,
            %(is_network_info_fetched)s, %(last_network_info_fetch)s, %(is_health_checked)s, %(last_health_check)s,
            %(is_whitelist_info_fetched)s, %(last_whitelist_info_fetch)s, %(address)s, %(remote_url)s, %(status)s, 
            %(status_at)s, %(inactive_at)s, %(moniker)s, %(is_residential)s, %(is_healthy)s, %(is_whitelisted)s, %(plan_id)s
        )
        """
        
        for plan, nodes in data_dict.items():
            print(plan)
            for node in nodes['data']:
                #print(nodes)
                # Ensure keys are in camelCase to match the column names in the table
                node_data = {
                    'id': node.get('id'),
                    'created_at': self.convert_datetime(node.get('created_at')),
                    'is_active': node.get('is_active'),
                    'revision': node.get('revision'),
                    'is_node_status_fetched': node.get('is_node_status_fetched'),
                    'last_node_status_fetch': self.convert_datetime(node.get('last_node_status_fetch')),
                    'is_network_info_fetched': node.get('is_network_info_fetched'),
                    'last_network_info_fetch': self.convert_datetime(node.get('last_network_info_fetch')),
                    'is_health_checked': node.get('is_health_checked'),
                    'last_health_check': self.convert_datetime(node.get('last_health_check')),
                    'is_whitelist_info_fetched': node.get('is_whitelist_info_fetched'),
                    'last_whitelist_info_fetch': self.convert_datetime(node.get('last_whitelist_info_fetch')),
                    'address': node.get('address'),
                    'remote_url': node.get('remote_url'),
                    'status': node.get('status'),
                    'status_at': self.convert_datetime(node.get('status_at')),
                    'inactive_at': self.convert_datetime(node.get('inactive_at')),
                    'moniker': node.get('moniker'),
                    'is_residential': node.get('is_residential'),
                    'is_healthy': node.get('is_healthy'),
                    'is_whitelisted': node.get('is_whitelisted'),
                    'plan_id': plan  # Add the plan number to the node data
                }
                #filled_query = c.mogrify(insert_query, node_data)
                #print(filled_query)
                c.execute(insert_query, node_data)
                db.commit()
    
    def convert_datetime(self, dt_str):
        """Convert datetime string to MySQL compatible format."""
        if dt_str is None:
            return None
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S.%f")
            
                
if __name__ == "__main__":
    pw = PlanWizard()
    db = pw.connDB()
    
    start = timer()
    plan_data = pw.getPlanWizardData()
    end = timer()
    
    time1 = round((end - start),4)
    
    print("It took %ss to get PlanWizardy Data " % (time1))
    
    start = timer()
    pw.insertPlanWizardData(db, plan_data)
    end = timer()
    
    time1 = round((end - start),4)
    
    print("It took %ss to insert PlanWizardy Data to database" % (time1))