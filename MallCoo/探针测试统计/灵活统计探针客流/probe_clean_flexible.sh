#!/bin/sh
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:$PATH


# 提供用法形式
#if [[ $# -ne 6 ]]; then
#  echo "6 parameters: date_partition, rssi_threshold, start_time, end_time, mall_id and area are required!" 
#  echo "For example: 20171208 -80 \'20171208 00:00:00\' \'20171208 23:59:59\' \'10024\' \'八吉岛\'"
#  exit 1
#fi
#
#set -e
#
#date_partition=$1
#rssi_threshold=$2 
#start_time=$3
#end_time=$4
#mall_id=$5
#area=$6

echo $date_partition
echo $rssi_threshold
echo $start_time
echo $end_time
echo $mall_id
echo $area


#hive -e "select mac, mallid, shopid, area, activity, bindtype, floorid from probe.probeinfo" > /apps/tony_test/probeinfo/probeinfo.txt

python /apps/tony_test/probe/probelocdata_source.py "$date_partition" $rssi_threshold "$start_time" "$end_time"

java -jar /apps/tony_test/java_jar/ProbeCleanFlexibe.jar "$date_partition" "$mall_id" "$area" "$start_time" "$end_time"

echo "客流统计完成！"