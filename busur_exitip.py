#!/bin/env python3

import requests
import pymysql
import json
import scrtsxx

VERSION = 20250319.1444

class BusurExitIP():
    def connDB(self): 
        
        self.db = pymysql.connect(host=scrtsxx.HOST,
                             port=scrtsxx.PORT,
                             user=scrtsxx.USERNAME,
                             passwd=scrtsxx.PASSWORD,
                             db=scrtsxx.DB,
                             charset="utf8mb4",
                             cursorclass=pymysql.cursors.DictCursor
                             )

    def upsert_nodes(self, node_data):
        cursor = self.db.cursor()
        
        query = """
        INSERT INTO exitip (
            addr, moniker, version, type, api, exitIp, asn, continentCode, countryCode, country,
            city, latitude, longitude, ipRep, isResidential, isActive, isHealthy, isDuplicate,
            isWhitelisted, fetchedAt, inactiveAt
        ) VALUES (
            %(addr)s, %(moniker)s, %(version)s, %(type)s, %(api)s, %(exitIp)s, %(asn)s,
            %(continentCode)s, %(countryCode)s, %(country)s, %(city)s, %(latitude)s,
            %(longitude)s, %(ipRep)s, %(isResidential)s, %(isActive)s, %(isHealthy)s,
            %(isDuplicate)s, %(isWhitelisted)s, %(fetchedAt)s, %(inactiveAt)s
        )
        ON DUPLICATE KEY UPDATE
            moniker = VALUES(moniker),
            version = VALUES(version),
            api = VALUES(api),
            exitIp = VALUES(exitIp),
            asn = VALUES(asn),
            continentCode = VALUES(continentCode),
            countryCode = VALUES(countryCode),
            country = VALUES(country),
            city = VALUES(city),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            ipRep = VALUES(ipRep),
            isResidential = VALUES(isResidential),
            isActive = VALUES(isActive),
            isHealthy = VALUES(isHealthy),
            isDuplicate = VALUES(isDuplicate),
            isWhitelisted = VALUES(isWhitelisted),
            fetchedAt = VALUES(fetchedAt),
            inactiveAt = VALUES(inactiveAt)
        """
        
        # Convert booleans to integers (MySQL TINYINT)
        for node in node_data:
            node['isResidential'] = 1 if node['isResidential'] else 0
            node['isActive'] = 1 if node['isActive'] else 0
            node['isHealthy'] = 1 if node['isHealthy'] else 0
            node['isDuplicate'] = 1 if node['isDuplicate'] else 0
            node['isWhitelisted'] = 1 if node['isWhitelisted'] else 0
        
            if node['inactiveAt'] is None:
                node['inactiveAt'] = None  
    
    
        cursor.executemany(query, node_data)
        self.db.commit()
        print(f"{cursor.rowcount} records affected")
        self.db.close()



if __name__ == "__main__":
    
    header = {"partnerKey" : scrtsxx.BUSUR_KEY}
    busur = BusurExitIP()
    busur.connDB()
    
    try:
        response = requests.get(scrtsxx.BUSUR, headers=header)
        response.raise_for_status()
        
        data = response.json()
        if data.get('success') and data.get('data'):
            nodes = data['data']
            busur.upsert_nodes(nodes)
        else:
            print("Invalid response format")
            
    except requests.exceptions.RequestException as e:
        print(f"API Request Failed: {e}")