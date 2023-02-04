USE meile;
REPLACE INTO ratings_nodes (node_address, score, votes) SELECT node_address, avg(rating), count(uuid) FROM ratings_user GROUP BY node_address;