#!/bin/bash
export  PATH=/apps/jdk/bin:/apps/hadoop/bin:/apps/hadoop/sbin:/apps/hive/bin:$PATH

# 提供用法形式
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 hive_table_partition_path." 
  echo "One parameter is required!" 
  echo "For example, sync_data_mongodb2hive.sh /user/hive/warehouse/tonytest.db/dataxtest3/src_file_day=20171108"
  exit 1
fi

set -e

hive_table_partition_path="$1"  #/user/hive/warehouse/tonytest.db/dataxtest3/src_file_day=20171108
echo "${hive_table_partition_path}" 

# 注意 数据日期(20171108)要比传入的时间参数(20171107)多一天, 因为mongo的时间用的是UTC时间，与北京时间差8个小时
src_file_day=`date -d "-1 day" +"%Y-%m-%d"`
echo "${src_file_day}"

# 判断分区是否存在，不存在则需手动创建
if hdfs dfs -ls ${hive_table_partition_path} 1> /dev/null 2>&1; then 
    echo "Hive table partition ${hive_table_partition_path} exists!"
else 
    echo "Hive table partition ${hive_table_partition_path} does not exist, you need create it!"
    exit 1
fi

# 判断分区数据是否存在，存在则删除老数据并导入新数据，不存在则直接导入新数据
if hdfs dfs -ls ${hive_table_partition_path}/* 1> /dev/null 2>&1; then 
    echo "Data in partirion ${hive_table_partition_path} exists, we should delete this old data!"
    echo "Delte old data!"
    hdfs dfs -rm ${hive_table_partition_path}/*
    # 执行导入数据的文件
    echo "Input new data!"
    /mnt/tonytest/datax/datax/bin/datax.py -p "-Dquery=\"${src_file_day}\" -Dpath=${hive_table_partition_path}" /mnt/tonytest/datax/datax/job/mongodb2hive.json
else
    echo "Input new data!" 
    /mnt/tonytest/datax/datax/bin/datax.py -p "-Dquery=\"${src_file_day}\" -Dpath=${hive_table_partition_path}" /mnt/tonytest/datax/datax/job/mongodb2hive.json
fi
