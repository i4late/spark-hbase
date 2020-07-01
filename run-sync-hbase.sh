#!/bin/bash
if [ $# -eq 1 ];
then
	spark-submit \
	--class com.missfresh.core.SparkReadHbase \
	--master yarn \
	--deploy-mode client \
	--driver-memory 1024M \
	--num-executors 2 \
	--executor-memory 1024M \
	--executor-cores 2 \
	spark-hbase-1.0-SNAPSHOT.jar \
	$1
else
    echo "需要传入绝对路径的配置yml文件"
fi
