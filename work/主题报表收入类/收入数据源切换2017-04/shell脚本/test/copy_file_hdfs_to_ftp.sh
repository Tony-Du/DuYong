#!/bin/bash

if [[ $# -ne 5 ]]; then
  echo "Usage: $FUNCNAME from_hdfs_path to_local_path to_file_name upload_ftp_path"
  exit 1
fi

set -e

hdfs_path="$1"
local_path="$2"
file_name="$3"
ftp_path="$4"
create_time=$(date '+%Y%m%d%H%M%S')

FTP_SERVER="172.20.37.35"
FTP_PORT="21"
FTP_USER="cpa013"
FTP_PWD="migucpa#013vd"

if hdfs dfs -ls ${hdfs_path}/* 1> /dev/null 2>&1; then
    echo "HDFS file ${hdfs_path}/* exists"
else
    echo "HDFS file ${hdfs_path}/* does not exist"
    exit 1
fi

if [ ! -d "${local_path}" ]; then
  umask 0022
  mkdir -p ${local_path}
fi

# 清理旧文件
if [ -f "${local_path}/.tmp.${file_name}" ]; then
  rm -f ${local_path}/.tmp.${file_name}
fi

for file in "${local_path}"/${file_name}*; do
  rm -f $file
done

# 从hdfs下载文件到本地
echo "cmd:hdfs dfs -getmerge file"
hdfs dfs -getmerge ${hdfs_path}/* ${local_path}/.tmp.${file_name}

file_bytes=`wc -c ${local_path}/.tmp.${file_name} | awk -F " " {'print $1'}`
echo "The file '.tmp.${file_name}' size:${file_bytes}"


# 生成校验文件信息:file_name|lines
for file in ${local_path}/${file_name}*; do
  data_file_name=`echo ${file} | awk -F"/" '{print $NF}'`
  lines=`wc -l ${file} | awk -F " " {'print $1'}`
  #bytes=`wc -c ${file} | awk -F " " {'print $1'}`
  echo "${data_file_name}|${lines}|${extract_day}|${create_time}" 1>>  ${local_path}/${file_name}.verf
done

echo "The verf file info:"
cat ${local_path}/${file_name}.verf


# 上传文件到ftp
ftp -n <<EOF 
open $FTP_SERVER $FTP_PORT
user $FTP_USER $FTP_PWD
passive
binary
cd $ftp_path
lcd $local_path
mput ${file_name}.*
close
bye
EOF


# 移动文件到备份路径,删除临时文件
mv ${local_path}/${file_name}.* ${local_path}/bak/
rm -f ${local_path}/.tmp.${file_name}*
rm -f ${local_path}/..tmp.${file_name}*

if [[ $? -ne 0 ]]; then
  exit 1
fi
