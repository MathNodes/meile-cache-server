#!/bin/bash

MYSQL_USER="username"
MYSQL_PASSWORD="password"


rm -rf update_ratings.sql
echo "USE meile;" >> update_ratings.sql
echo "REPLACE INTO ratings_nodes (node_address, score, votes) SELECT node_address, avg(rating), count(uuid) FROM ratings_user GROUP BY node_address;" >> update_ratings.sql
mysql --user=$MYSQL_USER --password=$MYSQL_PASSWORD < update_ratings.sql