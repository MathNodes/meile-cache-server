USE meile;
UPDATE node_score AS ns, (SELECT node_address FROM node_score WHERE isp LIKE "Amazon%") as temp SET ns.datacenter = True WHERE temp.node_address = ns.node_address;
UPDATE node_score AS ns, (SELECT node_address FROM node_score WHERE isp LIKE "Hetzner%") as temp SET ns.datacenter = True WHERE temp.node_address = ns.node_address;
UPDATE node_score AS ns, (SELECT node_address FROM node_score WHERE isp LIKE "OVH%") as temp SET ns.datacenter = True WHERE temp.node_address = ns.node_address;
UPDATE node_score AS ns, (SELECT node_address FROM node_score WHERE isp LIKE "Digitalocean%") as temp SET ns.datacenter = True WHERE temp.node_address = ns.node_address;
UPDATE node_score AS ns, (SELECT node_address FROM node_score WHERE isp LIKE "Contabo%") as temp SET ns.datacenter = True WHERE temp.node_address = ns.node_address;
