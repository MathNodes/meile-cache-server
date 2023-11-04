SELECT t1.node_address, t1.remote_url, t2.node_address, t2.remote_url
FROM node_uptime t1
JOIN node_uptime t2 ON t1.remote_url = t2.remote_url
WHERE t1.node_address <> t2.node_address;
