#!/usr/bin/env python3
import pymysql
import json
import geoip2.database
import scrtsxx

def connDB():
    db = pymysql.connect(host=scrtsxx.HOST,
                         port=scrtsxx.PORT,
                         user=scrtsxx.USERNAME,
                         passwd=scrtsxx.PASSWORD,
                         db=scrtsxx.DB,
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    return db


def getMeilePing(db):
    
    cursorObject = db.cursor()
    query = "SELECT * from meileping"
    cursorObject.execute(query)
    
    return cursorObject.fetchall()                                     


def ScrubIPsFromMeileDB(db):
    
    scrub_query = 'UPDATE meileping SET ip = NULL;'
    c = db.cursor()
    c.execute(scrub_query)
    db.commit()

def UpdateMeileGEOIP(MeilePingDB, db):
    GEOIPDict = {'uuid' : None, 'country' : None, 'region' : None, 'city' : None, 'latitude' : None, 'longitude' : None}
    for row in MeilePingDB:
        print(row)
        uuid = row['uuid']
        ip   = row['ip']
        if ip:
            with geoip2.database.Reader('GeoLite2-City.mmdb') as reader:
                response = reader.city(ip)
                GEOIPDict['uuid'] = uuid
                GEOIPDict['country'] = response.country.name
                GEOIPDict['region'] = response.subdivisions.most_specific.name
                GEOIPDict['city'] = response.city.name
                GEOIPDict['latitude'] = response.location.latitude
                GEOIPDict['longitude'] = response.location.longitude
            query = '''INSERT IGNORE INTO meile_geoip (uuid, country, region, city, latitude, longitude)
                       VALUES ("%s", "%s", "%s", "%s", "%s", "%s")
                    ''' % (GEOIPDict['uuid'],
                           GEOIPDict['country'],
                           GEOIPDict['region'],
                           GEOIPDict['city'],
                           GEOIPDict['latitude'],
                           GEOIPDict['longitude'])
            print(query)
            cursor = db.cursor()
            cursor.execute(query)
            db.commit()

if __name__ == "__main__":
    db = connDB()
    MeilePingDB = getMeilePing(db)
    UpdateMeileGEOIP(MeilePingDB, db)
    ScrubIPsFromMeileDB(db)
    
    


