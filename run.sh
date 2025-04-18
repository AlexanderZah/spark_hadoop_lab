#!/bin/bash

FILE="steambans.csv"
DATA_PATH="hdfs://namenode:9000/data/steambans.csv"

docker-compose -f docker-compose.yml up -d

docker cp ./data/$FILE namenode:/

docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put -f ./data/$FILE /data

docker cp ./src/main.py spark-master:/tmp/main.py

docker exec spark-worker-1 apk add --no-cache make automake gcc g++ python3-dev linux-headers
docker exec spark-master apk add --no-cache make automake gcc g++ python3-dev linux-headers py3-pip
docker exec spark-master pip3 install psutil


docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/main.py -d $DATA_PATH | tail -n 6 >> res_one_node.txt
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/main.py -d $DATA_PATH -o | tail -n 6 >> res_one_node_opt.txt


docker-compose stop

docker-compose -f docker-compose-3d.yml up -d

docker cp ./data/$FILE namenode:/

docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put -f ./data/$FILE /data

docker cp ./src/main.py spark-master:/tmp/main.py

docker exec spark-master pip3 install psutil


docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/main.py -d $DATA_PATH | tail -n 6 >> res_three_node.txt
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/main.py -d $DATA_PATH -o | tail -n 6 >> res_three_node_opt.txt


docker-compose stop

