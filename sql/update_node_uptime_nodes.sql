USE meile;
INSERT IGNORE INTO node_uptime (node_address) SELECT node_address FROM node_cities;
INSERT IGNORE INTO node_geoip (node_address) SELECT node_address FROM node_cities;
