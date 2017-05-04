#!/bin/bash

if [[ $# -ne 4 ]]; then
  echo "Usage: $FUNCNAME from_hdfs_path to_temp_local_path file_name des_local_path"
  exit 1
fi

set -e

hdfs_path="$1"		    # /user/hive/warehouse/app.db/user_use_detail_sx0290_monthly/src_file_month=201703 
temp_local_path="$2"	# /mnt/cdmp_out/cdmp/ftp/gesheng/shaanxi/auto_fare/.tmp
file_name="$3"		    # user_use_detail_m-201703
local_path="$4"		    # /mnt/cdmp_out/cdmp/ftp/gesheng/shaanxi/auto_fare	

data_time=`echo ${file_name} | awk -F "-" {'print $2'}`

# 判断hdfs路径是否存在
if hdfs dfs -ls ${hdfs_path}/* 1> /dev/null 2>&1; then
    echo "HDFS file ${hdfs_path}/* exists"
else
    echo "HDFS file ${hdfs_path}/* does not exist"
    exit 1
fi


# 判断本地目标路径是否存在
if ls ${local_path}/${data_time} 1> /dev/null 2>&1; then
    echo "The directory ${local_path}/${data_time} exists"
else 
    echo "The directory ${local_path}/${data_time} does not exist, creates it"
	mkdir -p ${local_path}/${data_time}
fi

#if [ ! -d "${local_path}/${data_time}" ]; then
#  umask 0022
#  mkdir -p ${local_path}/${data_time}
#fi


# 从hdfs下载文件到本地
echo "cmd:hdfs dfs -getmerge file"
hdfs dfs -getmerge ${hdfs_path}/* ${temp_local_path}/.tmp.${file_name}

file_bytes=`wc -c ${temp_local_path}/.tmp.${file_name} | awk -F " " {'print $1'}`
echo "The file '.tmp.${file_name}' size:${file_bytes}"


# 分割大文件
if [ ${file_bytes} -gt 1000 ]; then
  echo "cmd:split -a 6 -d -C 200M ${temp_local_path}/.tmp.${file_name} ${temp_local_path}/${file_name}-"
  split -a 6 -d -C 200M ${temp_local_path}/.tmp.${file_name} ${temp_local_path}/${file_name}-
  for subfile in ${temp_local_path}/${file_name}*; do
    n=1
    seqnum=`echo ${subfile} | awk -F "-" -v n=$n '{printf("%06d",$NF+n)}'`
    mv ${subfile} ${temp_local_path}/${file_name}-${seqnum}.dat
  done
else
   mv ${temp_local_path}/.tmp.${file_name} ${temp_local_path}/${file_name}-000001.dat
fi


# 生成校验文件信息:file_name|bytes|lines|data_time
for file in ${temp_local_path}/${file_name}*; do
  data_file_name=`echo ${file} | awk -F"/" '{print $NF}'`
  lines=`wc -l ${file} | awk -F " " {'print $1'}`
  bytes=`wc -c ${file} | awk -F " " {'print $1'}`
  #data_time=`echo ${file} | awk -F "-" {'print $2'}`
  echo "${data_file_name}|${bytes}|${lines}|${data_time}" 1>> ${temp_local_path}/${file_name}.verf
done
echo "999999" 1>> ${temp_local_path}/${file_name}.verf

echo "The verf file info:"
cat ${temp_local_path}/${file_name}.verf


# 移动文件到目的路径,删除临时文件
mv ${temp_local_path}/${file_name}* ${local_path}/${data_time}/
rm -f ${temp_local_path}/.tmp.${file_name}*
rm -f ${temp_local_path}/..tmp.${file_name}*

if [[ $? -ne 0 ]]; then
  exit 1
fi