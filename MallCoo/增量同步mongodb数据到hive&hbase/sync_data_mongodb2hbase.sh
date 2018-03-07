#!/bin/bash
export  PATH=/apps/jdk/bin:/apps/hadoop/bin:/apps/hadoop/sbin:/apps/hive/bin:$PATH

set -e

# 注意 数据日期(20171108)要比传入的时间参数(20171107)多一天, 因为mongo的时间用的是UTC时间，与北京时间差8个小时
src_file_day=`date -d "-1 day" +"%Y-%m-%d"` 
echo "${src_file_day}"

echo "Input new data !"
/mnt/tonytest/datax/datax/bin/datax.py -p "-Dquery=\"${src_file_day}\"" /mnt/tonytest/datax/datax/job/mongodb2hbase.json
