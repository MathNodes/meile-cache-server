#!/usr/bin/env python3
import pymysql
import urllib.parse
import ipaddress
import socket
from datetime import datetime
import scrtsxx

# ================== CONFIGURATION ==================
DB_CONFIG = {
    'host': scrtsxx.HOST,
    'port': scrtsxx.PORT,
    'user': scrtsxx.USERNAME,
    'password': scrtsxx.PASSWORD,
    'database': scrtsxx.DB,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}
# ===================================================

def extract_ip(remote_url: str) -> str:
    if not remote_url:
        return ""

    try:
        # Handle URLs that may not have a scheme
        if not remote_url.startswith(('http://', 'https://')):
            url_to_parse = '//' + remote_url
        else:
            url_to_parse = remote_url

        result = urllib.parse.urlsplit(url_to_parse)
        hostname = result.hostname
        if not hostname:
            return ""

        # Try as direct IP first
        try:
            ip = ipaddress.ip_address(hostname)
            return str(ip)
        except ValueError:
            pass

        # Fall back to DNS lookup
        try:
            ip = socket.gethostbyname(hostname)
            return str(ip)
        except (socket.gaierror, TypeError, OSError):
            return ""

    except Exception:
        return ""

def main():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # Fetch all nodes
        cur.execute("SELECT * FROM node_score")
        nodes = cur.fetchall()

        updated_count = 0
        for node in nodes:
            node_address = node['node_address']

            # Get remote_url from node_uptime
            cur.execute(
                "SELECT remote_url FROM node_uptime WHERE node_address = %s",
                (node_address,)
            )
            row = cur.fetchone()
            remote_url = row['remote_url'] if row and row['remote_url'] else None

            ip_value = extract_ip(remote_url)

            # Insert/Update
            cur.execute("""
                INSERT INTO nodes_ip 
                (node_address, asn, isp, isp_type, datacenter, last_checked, ip)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    asn = VALUES(asn),
                    isp = VALUES(isp),
                    isp_type = VALUES(isp_type),
                    datacenter = VALUES(datacenter),
                    last_checked = VALUES(last_checked),
                    ip = VALUES(ip)
            """, (
                node_address,
                node.get('asn'),
                node.get('isp'),
                node.get('isp_type'),
                node.get('datacenter'),
                node.get('last_checked'),
                ip_value
            ))
            updated_count += 1

        conn.commit()
        print(f"{datetime.now()}: Successfully updated {updated_count} records in nodes_ip table.")

    except Exception as e:
        print(f"{datetime.now()}: Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()