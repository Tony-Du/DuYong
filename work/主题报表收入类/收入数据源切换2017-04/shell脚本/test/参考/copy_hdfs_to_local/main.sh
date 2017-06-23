#!/bin/bash

if [[ $# -ne 4 ]]; then	#参数个数 不等于 4
  echo "Usage: $FUNCNAME from_hdfs_path to_local_path to_file_name extract_day"
  exit 1
fi

hdfs_path="$1"
local_path="$2"
file_name="$3"
extract_day="$4"
create_time=$(date '+%Y%m%d%H%M%S')
  
if hdfs dfs -ls ${hdfs_path}/* 1> /dev/null 2>&1; then	
#标准输出 重定向到 空设备文件，标准错误输出 重定向到 标准输出，即空设备文件;判断该命令是否执行成功
    echo "HDFS file ${hdfs_path}/* exists"
else
    echo "HDFS file ${hdfs_path}/* does not exist"
    exit 1
fi


mkdir -p ${local_path}	

hdfs dfs -getmerge ${hdfs_path}/* ${local_path}/${file_name}.temp

lines=`wc -l ${local_path}/${file_name}.temp | awk -F " " {'print $1'}`	
bytes=`wc -c ${local_path}/${file_name}.temp | awk -F " " {'print $1'}`



seqnum="00"
if ls ${local_path}/${file_name}*.dat 1> /dev/null 2>&1; then
   seqnum=`ls -l ${local_path}/${file_name}*.dat | tail -1 | awk -F " " {'print $9'} | awk -F "_" {'print $(NF-1)'} | awk -F "." {'print $1'}`   
   seqnum=$(($seqnum + 1))
   seqnum="0$seqnum"		#左边补0，拼接
   seqnum=${seqnum:(-2)}    #取后面两位
fi

mv ${local_path}/${file_name}.temp ${local_path}/${file_name}_${seqnum}_001.dat

if [[ $? -ne 0 ]]; then
  exit 1
fi

#filename|bytes|lines|extract_day|create_time
echo "${file_name}_${seqnum}_001.dat|${bytes}|${lines}|${extract_day}|${create_time}"> ${local_path}/${file_name}_${seqnum}.verf
