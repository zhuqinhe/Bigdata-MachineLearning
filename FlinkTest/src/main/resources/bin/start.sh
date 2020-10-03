#!/bin/sh

export HADOOP_CONF_DIR=/etc/hadoop/conf.cloudera.hdfs
export FLINK_HOME=/opt/hoob/3RD/flink
export YARN_CONF_DIR=/etc/hadoop/conf.cloudera.yarn
export  PATH=$FLINK_HOME/bin:/opt/cloudera/parcels/CDH/bin/:$PATH

base_dir=$(dirname $0)

ES_ENV_FILE=$base_dir/env.sh
if [ -f "$ES_ENV_FILE" ]; then
    . "$ES_ENV_FILE"
fi

NE_HOME="/opt/hoob/NE/flink/etc"
#stop
$NE_HOME/bin/stop.sh

#start

command="flink run -m yarn-cluster -ynm FlinkTest -nm FlinkTest -yn $taskmanagerNumber -ys $taskmanagerNumberOfTaskSlots -ytm $taskmanagerHeapMb $NE_HOME/FlinkTest.jar"
now=$(date +%Y%m%d)
echo "$now start"
exec $command > $NE_HOME/logs/log.log 2>&1 &
echo "$now start FlinkTest success."

