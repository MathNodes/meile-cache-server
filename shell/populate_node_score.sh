#!/bin/bash

USERNAME="mathnodes"
PASSOWRD="password"
SQLFILE="/home/sentinel/SQL/update_node_score_from_online_nodes.sql"

mysql --user=$USERNAME --password=$PASSWORD < $SQLFILE
