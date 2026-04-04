#!/bin/env python3

import requests
import random
import scrtsxx
import socket
import pymysql
import ipaddress
from time import sleep
from datetime import datetime, timedelta
import concurrent.futures
from dbutils.pooled_db import PooledDB

APIKEYS = scrtsxx.IP_REGISTRY_API_KEYS

VERSION = 20260404.0002
APIURL = 'https://api.sentinel.mathnodes.com'

IPREGISTRY_URL = "https://api.ipregistry.co/%s?key=%s"

class UpdateNodeType():
    NodeAPIurl = {}

    def connDB(self):

        self.db_pool = PooledDB(
            creator=pymysql,
            maxconnections=10,
            host=scrtsxx.HOST,
            port=scrtsxx.PORT,
            user=scrtsxx.USERNAME,
            passwd=scrtsxx.PASSWORD,
            database=scrtsxx.DB,
            cursorclass=pymysql.cursors.DictCursor,
        )

        db = pymysql.connect(
            host=scrtsxx.HOST,
            port=scrtsxx.PORT,
            user=scrtsxx.USERNAME,
            passwd=scrtsxx.PASSWORD,
            db=scrtsxx.DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )

        return db

    def get_node_type_table(self, db):
        query = "SELECT * FROM node_score;"
        c = db.cursor()
        c.execute(query)
        return c.fetchall()

    def _needs_check(self, last_checked):
        """Return True if last_checked is NULL or older than
        7 days."""
        if last_checked is None:
            return True
        if isinstance(last_checked, str):
            last_checked = datetime.strptime(
                last_checked, "%Y-%m-%d %H:%M:%S"
            )
        return datetime.utcnow() - last_checked > timedelta(days=7)

    def _resolve_remote_url(self, remote_url_raw):
        """Extract host from a remote_url and resolve to an
        ipaddress object.  Handles IPv4, IPv6 (bracketed), and
        hostnames.  Returns an ipaddress object or raises
        ValueError/socket.gaierror on failure."""

        if not remote_url_raw:
            raise ValueError("Empty remote_url")

        # Strip scheme
        host_part = remote_url_raw.split('//')[-1]

        # Handle bracketed IPv6, e.g. [2001:db8::1]:8585
        if host_part.startswith('['):
            bracket_end = host_part.index(']')
            host = host_part[1:bracket_end]
        else:
            host = host_part.rsplit(':', 1)[0]

        # Try parsing as a literal IP first (v4 or v6)
        try:
            return ipaddress.ip_address(host)
        except ValueError:
            pass

        # It's a hostname — resolve it
        results = socket.getaddrinfo(
            host, None, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        if not results:
            raise socket.gaierror(
                f"Could not resolve hostname: {host}"
            )

        # Prefer IPv4 if available, otherwise take the first result
        for family, _, _, _, sockaddr in results:
            if family == socket.AF_INET:
                return ipaddress.ip_address(sockaddr[0])
        return ipaddress.ip_address(results[0][4][0])

    def _ensure_node_uptime_row(self, db, address, remote_url=None):
        """Insert a row into node_uptime if one does not already
        exist for this node_address.  If remote_url is provided,
        store it; otherwise store NULL."""

        connection = self.db_pool.connection()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT node_address, remote_url FROM node_uptime "
                "WHERE node_address = %s;",
                (address,),
            )
            existing = cursor.fetchone()
            if not existing:
                print(
                    f"Inserting missing node_uptime row for "
                    f"{address} with remote_url={remote_url}"
                )
                cursor.execute(
                    "INSERT INTO node_uptime "
                    "(node_address, remote_url, tries, success, "
                    "success_rate) VALUES (%s, %s, 0, 0, 0.000);",
                    (address, remote_url),
                )
                connection.commit()
            elif (
                remote_url
                and not existing.get('remote_url')
            ):
                cursor.execute(
                    "UPDATE node_uptime SET remote_url = %s "
                    "WHERE node_address = %s;",
                    (remote_url, address),
                )
                connection.commit()
        finally:
            cursor.close()
            connection.close()

    def _touch_last_checked(self, node_address):
        """Update last_checked to NOW() without calling the
        ipregistry API.  Used when the node IP data has not
        changed and we can safely assume the ASN/ISP info
        is still valid."""

        connection = self.db_pool.connection()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "UPDATE node_score SET last_checked = NOW() "
                "WHERE node_address = %s;",
                (node_address,),
            )
            connection.commit()
            print(
                f"Touched last_checked for {node_address} "
                f"(no API call needed)"
            )
        finally:
            cursor.close()
            connection.close()

    def _has_asn_data(self, node_data):
        """Return True if this node already has ASN/ISP data
        populated in the node_score row."""
        return (
            node_data.get('asn') is not None
            and node_data.get('isp') is not None
            and node_data.get('isp_type') is not None
        )

    def get_ip_of_node(self, db, NodeData):
        NodeDBIP = {}
        NodeIP = {}
        NodeIPURLChanged = {}
        touched_count = 0

        c = db.cursor()

        self.api_rurl_multithread(NodeData)

        for n in NodeData:
            address = n['node_address']
            last_checked = n.get('last_checked', None)

            # ---------------------------------------------------
            # 1) Get the remote_url stored in node_uptime (if any)
            # ---------------------------------------------------
            c.execute(
                "SELECT remote_url FROM node_uptime "
                "WHERE node_address = %s;",
                (address,),
            )
            result = c.fetchone()

            # ---------------------------------------------------
            # 2) Get the exitIp (if any)
            # ---------------------------------------------------
            c.execute(
                "SELECT exitIp FROM exitip WHERE addr = %s;",
                (address,),
            )
            exitresult = c.fetchone()

            # ---------------------------------------------------
            # 3) Determine db_rurl and api_rurl
            # ---------------------------------------------------
            db_rurl = ""
            if result and result.get('remote_url'):
                db_rurl = result['remote_url']

            api_rurl = self.NodeAPIurl.get(address, "")

            # ---------------------------------------------------
            # 4) Ensure node_uptime has a row for this node
            # ---------------------------------------------------
            self._ensure_node_uptime_row(
                db, address, api_rurl if api_rurl else None
            )

            # ---------------------------------------------------
            # 5) Determine effective remote_url and whether it
            #    changed
            # ---------------------------------------------------
            effective_rurl = api_rurl if api_rurl else db_rurl
            url_changed = (
                db_rurl != ""
                and api_rurl != ""
                and db_rurl != api_rurl
            )

            if url_changed:
                self.__UpdateUptimeTable(db, address, api_rurl)

            # ---------------------------------------------------
            # 6) Resolve remote_url to an IP
            # ---------------------------------------------------
            rurlip = None
            if effective_rurl:
                try:
                    rurlip = self._resolve_remote_url(
                        effective_rurl
                    )
                except (ValueError, socket.gaierror) as e:
                    print(
                        f"Could not resolve {effective_rurl} "
                        f"for {address}: {e}"
                    )

            # ---------------------------------------------------
            # 7) Resolve exitIp
            # ---------------------------------------------------
            exit_ip = None
            try:
                if exitresult and exitresult.get('exitIp'):
                    exit_ip = ipaddress.ip_address(
                        exitresult['exitIp']
                    )
            except (ValueError, KeyError):
                pass

            # ---------------------------------------------------
            # 8) Determine the IP to use and whether there is a
            #    mismatch
            # ---------------------------------------------------
            ip_mismatch = False
            chosen_ip = None

            if rurlip and exit_ip:
                ip_mismatch = rurlip != exit_ip
                chosen_ip = exit_ip if ip_mismatch else rurlip
            elif rurlip:
                chosen_ip = rurlip
            elif exit_ip:
                chosen_ip = exit_ip

            if ip_mismatch:
                print(f"{rurlip},{exit_ip}")

            if chosen_ip is None:
                # No IP at all — nothing we can do
                continue

            # ---------------------------------------------------
            # 9) Decide whether this node needs an API check
            #
            # Cases that REQUIRE an ipregistry API call:
            #   a) IP mismatch (exitIp != rurlip)
            #   b) URL changed to a new remote_url
            #   c) ASN/ISP data is NULL (never been checked)
            #
            # Case that does NOT require an API call:
            #   - last_checked is stale (>7 days) BUT the
            #     remote_url is unchanged, IPs match, and
            #     ASN data is already populated.
            #     -> Just touch last_checked.
            # ---------------------------------------------------
            needs_time_check = self._needs_check(last_checked)
            has_data = self._has_asn_data(n)

            if ip_mismatch or url_changed:
                # Something changed — must call the API
                if url_changed:
                    NodeIPURLChanged[address] = chosen_ip
                else:
                    NodeIP[address] = chosen_ip

            elif not has_data:
                # Never been checked — must call the API
                NodeIP[address] = chosen_ip

            elif needs_time_check and has_data:
                # Stale but nothing changed and data exists
                # — just refresh the timestamp, no API call
                self._touch_last_checked(address)
                touched_count += 1

            # else: last_checked is fresh, data exists,
            #        nothing changed — skip entirely

        print(
            f"Nodes to check (unchanged URL): {len(NodeIP)}, "
            f"Nodes to check (changed URL): "
            f"{len(NodeIPURLChanged)}, "
            f"Nodes touched (no API call): {touched_count}"
        )
        return NodeIP, NodeIPURLChanged

    def check_asn_null(self, db, node_address):
        connection = self.db_pool.connection()
        cursor = connection.cursor()

        try:
            cursor.execute(
                "SELECT asn FROM node_score "
                "WHERE node_address = %s;",
                (node_address,),
            )
            result = cursor.fetchone()
        finally:
            cursor.close()
            connection.close()

        if result and result.get('asn') is None:
            return True
        return False

    def api_rurl_multithread(self, NodeData):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=20
        ) as executor:
            futures = [
                executor.submit(
                    self.__api_url_worker, node['node_address']
                )
                for node in NodeData
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print("An error occurred:", str(e))

    def ip_registry_multithread(self, db, NodeIP, changed):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=3
        ) as executor:
            futures = [
                executor.submit(
                    self.__ip_registry_worker,
                    node, ip, db, changed,
                )
                for node, ip in NodeIP.items()
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print("An error occurred:", str(e))

    def __api_url_worker(self, address):
        endpoint = (
            APIURL + '/sentinel/node/v3/nodes/' + address
        )

        try:
            r = requests.get(endpoint, timeout=10)
            api_rurl = r.json()['node']['remote_addrs'][0]
        except Exception as e:
            print(f"API URL WORKER ERROR ({address}): {str(e)}")
            api_rurl = ""

        self.NodeAPIurl[address] = api_rurl

    def __ip_registry_worker(self, node, ip, db, changed):
        if changed:
            print(f"CHANGED REMOTE URL: checking {node}...")
            self.__check_nodes(node, ip, db)
        elif self.check_asn_null(db, node):
            print(f"NULL ASN: checking {node}...")
            self.__check_nodes(node, ip, db)
        else:
            print(f"STALE last_checked: refreshing {node}...")
            self.__check_nodes(node, ip, db)

    def __check_nodes(self, node, ip, db):
        N = random.randint(0, len(APIKEYS) - 1)
        API_KEY = APIKEYS[N]
        TYPE = {
            "residential": False,
            "business": False,
            "hosting": False,
            "education": False,
            "government": False,
        }
        try:
            resp = requests.get(
                IPREGISTRY_URL % (ip, API_KEY), timeout=10
            )
            rJSON = resp.json()
            sleep(0.3)
        except Exception as e:
            print(
                f"ipregistry error for {node} ({ip}): {str(e)}"
            )
            return
        try:
            ASN = "AS" + str(rJSON['connection']['asn'])
            ISP = rJSON['connection']['organization']

            if rJSON['security']['is_cloud_provider']:
                TYPE['hosting'] = True

            elif rJSON['company']['type'] == "isp":
                if rJSON['connection']['type'] == "isp":
                    TYPE['residential'] = True
                elif rJSON['connection']['type'] == "business":
                    TYPE['business'] = True
                else:
                    TYPE['hosting'] = True

            elif rJSON['company']['type'] == "business":
                if rJSON['connection']['type'] == "hosting":
                    TYPE['hosting'] = True
                else:
                    TYPE['business'] = True

            elif rJSON['company']['type'] == "education":
                TYPE['education'] = True

            elif rJSON['company']['type'] == "government":
                TYPE['government'] = True

            for k, v in TYPE.items():
                if v:
                    self.__UpdateNodeTypeTable(
                        db, node, k, ASN, ISP
                    )
        except KeyError as e:
            print(
                f"KeyError for {node} ({ip}): {str(e)} — "
                f"response: {rJSON}"
            )

    def __UpdateNodeTypeTable(self, db, node, ntype, asn, isp):
        with open('node_whitelist', "r") as wlFILE:
            whitelist = [line.strip() for line in wlFILE]

        if node in whitelist:
            return

        connection = self.db_pool.connection()
        cursor = connection.cursor()

        query = (
            "UPDATE node_score "
            "SET asn = %s, isp = %s, isp_type = %s, "
            "last_checked = NOW() "
            "WHERE node_address = %s;"
        )
        print(
            f"Updating node_score: {node} -> "
            f"asn={asn}, isp={isp}, type={ntype}"
        )
        try:
            cursor.execute(query, (asn, isp, ntype, node))
            connection.commit()
        finally:
            cursor.close()
            connection.close()

    def __UpdateUptimeTable(self, db, node, rurl):
        print(
            f"Updating node_uptime remote_url for {node}: "
            f"{rurl}"
        )
        c = db.cursor()
        c.execute(
            "UPDATE node_uptime SET remote_url = %s "
            "WHERE node_address = %s;",
            (rurl, node),
        )
        db.commit()

if __name__ == "__main__":
    NType = UpdateNodeType()
    db = NType.connDB()
    NodeData = NType.get_node_type_table(db)
    print(f"Total nodes in node_score: {len(NodeData)}")
    NodeIP, URLsChanged = NType.get_ip_of_node(db, NodeData)
    print(
        f"Processing {len(NodeIP)} nodes with unchanged "
        f"URLs..."
    )
    NType.ip_registry_multithread(db, NodeIP, False)
    print(
        "--------------- Computing URLs Changed "
        "-------------------"
    )
    print(
        f"Processing {len(URLsChanged)} nodes with "
        f"changed URLs..."
    )
    NType.ip_registry_multithread(db, URLsChanged, True)
    print("Done.")