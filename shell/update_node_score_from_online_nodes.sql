USE meile;
INSERT IGNORE INTO node_score (node_address) SELECT node_address FROM online_nodes;
