#!/bin/bash

USERNAME="username"
PASSWORD="password"

#/home/sentinel/CacheServer/meile_node_score.py
cd /home/sentinel/CacheServer
python3 /home/sentinel/CacheServer/meile_intelligence.py
#whitelist/blacklist
mysql --user=$USERNAME --password=$PASSWORD  < /home/sentinel/SQL/update_node_score.sql
python3 /home/sentinel/CacheServer/remove_dupe_resis.py