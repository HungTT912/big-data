#!/bin/bash

hadoop fs -mkdir       /tmp
hadoop fs -mkdir -p    /user/hive/warehouse
hadoop fs -chmod g+w   /tmp
hadoop fs -chmod g+w   /user/hive/warehouse

# start ssh server
service ssh start

cd $HIVE_HOME/bin
./hiveserver2 --hiveconf hive.server2.enable.doAs=false