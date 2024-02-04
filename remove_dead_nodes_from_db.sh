#!/bin/bash
#
# This script removes nodes that have an uptime less than 5%
# from the relevant tables within the Meile Cache System
#
# We store 3 months worth of nodes and this script runs
# on the last day of the month every three months. 



mysql --defaults-extra-file=/home/sentinel/SQL/mysqldb.conf <<EOF 
USE meile
DELETE node_cities
FROM node_cities
INNER JOIN node_uptime ON node_cities.node_address = node_uptime.node_address
WHERE node_uptime.success_rate < 0.05;
EOF

mysql --defaults-extra-file=/home/sentinel/SQL/mysqldb.conf <<EOF 
USE meile
DELETE node_geoip
FROM node_geoip
INNER JOIN node_uptime ON node_geoip.node_address = node_uptime.node_address
WHERE node_uptime.success_rate < 0.05;
EOF

mysql --defaults-extra-file=/home/sentinel/SQL/mysqldb.conf <<EOF 
USE meile
DELETE node_score
FROM node_score
INNER JOIN node_uptime ON node_score.node_address = node_uptime.node_address
WHERE node_uptime.success_rate < 0.05;
EOF

mysql --defaults-extra-file=/home/sentinel/SQL/mysqldb.conf <<EOF 
USE meile
DELETE 
FROM node_uptime
WHERE node_uptime.success_rate < 0.05;
EOF

