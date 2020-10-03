#!/bin/sh

export HADOOP_CONF_DIR=/etc/hadoop/conf.cloudera.hdfs
export FLINK_HOME=/opt/hoob/3RD/flink
export YARN_CONF_DIR=/etc/hadoop/conf.cloudera.yarn
export  PATH=$FLINK_HOME/bin:/opt/cloudera/parcels/CDH/bin/:$PATH

yarn_application_id=`yarn --config /etc/hadoop/conf.cloudera.yarn application --list | grep flink-ott-user-online-stat | awk -F ' ' '{print $1}'`
if [[ -z $yarn_application_id ]];
then
    echo "stopped."
else
    echo "started."
fi

