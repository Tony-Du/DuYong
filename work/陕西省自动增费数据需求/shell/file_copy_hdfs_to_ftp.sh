#!/bin/bash

if [[ $# -ne 4 ]]; then
  echo "Usage: $FUNCNAME from_hdfs_path to_local_path to_file_name upload_ftp_path"
  exit 1
fi

set -e

hdfs_path="$1"      # /user/hive/warehouse/app.db/user_use_detail_sx0290_monthly/src_file_month=201703 
local_path="$2"     # /home/hadoop/tony_test/user_use_visit
file_name="$3"      # user_use_detail_m-201703
ftp_path="$4"       # /home/thadoop/tony_test/data

FTP_SERVER="10.200.65.50"
FTP_PORT="21"
FTP_USER="thadoop"
FTP_PWD="thadoop123!@#"

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

# 从hdfs下载文件到本地
echo "cmd:hdfs dfs -getmerge file"
hdfs dfs -getmerge ${hdfs_path}/* ${local_path}/.tmp.${file_name}

file_bytes=`wc -c ${local_path}/.tmp.${file_name} | awk -F " " {'print $1'}`
echo "The file '.tmp.${file_name}' size:${file_bytes}"

# 分割大文件
if [ ${file_bytes} -gt 1000 ]; then
  echo "cmd:split -a 6 -d -c 200M ${local_path}/.tmp.${file_name} ${local_path}/${file_name}-"
  split -a 6 -d -c 200M ${local_path}/.tmp.${file_name} ${local_path}/${file_name}-
  
  for subfile in ${local_path}/${file_name}*; do
    n=1
    seqnum=`echo ${subfile} | awk -F "-" -v n=$n '{printf("%06d",$NF+n)}'` #为什么使用print不行, mv: target `1.dat' is not a directory。原因：print不支持"%06d"格斯输出
    mv ${subfile} ${local_path}/${file_name}-${seqnum}.dat
  done
 
else
  mv ${local_path}/.tmp.${file_name} ${local_path}/${file_name}-000001.dat
fi


# 生成校验文件信息:file_name|lines
for file in ${local_path}/${file_name}*; do
  data_file_name=`echo ${file} | awk -F"/" '{print $NF}'`
  lines=`wc -l ${file} | awk -F " " {'print $1'}`
  bytes=`wc -c ${file} | awk -F " " {'print $1'}`
  data_time=`echo ${file} | awk -F "-" {'print $2'}`
  echo "${data_file_name}|${bytes}|${lines}|${data_time}" 1>> ${local_path}/${file_name}.verf
done
echo "999999" 1>> ${local_path}/${file_name}.verf

echo "The verf file info:"
cat ${local_path}/${file_name}.verf

#data_time=`echo ${local_path}/${file_name}* | awk -F "-" {'print $2'}` 这里会出现
#201703
#201703
#201703
#201703
#201703.verf

data_time=`echo ${file_name} | awk -F "-" {'print $2'}`		

# 上传文件到ftp
ftp -n <<EOF 
open $FTP_SERVER $FTP_PORT
user $FTP_USER $FTP_PWD
prompt off
binary
mkdir $ftp_path/$data_time   
cd $ftp_path/$data_time
lcd $local_path
mput ${file_name}*
close
bye
EOF

# 移动文件到备份路径,删除临时文件
mv ${local_path}/${file_name}* ${local_path}/bak/
rm -f ${local_path}/.tmp.${file_name}*
rm -f ${local_path}/..tmp.${file_name}*

if [[ $? -ne 0 ]]; then
  exit 1
fi

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

#!/bin/bash

if [[ $# -ne 4 ]]; then
  echo "Usage: $FUNCNAME from_hdfs_path to_local_path to_file_name upload_ftp_path"
  exit 1
fi

set -e

hdfs_path="$1"		# /user/hive/warehouse/app.db/user_use_detail_sx0290_monthly/src_file_month=201703 
local_path="$2"		# /home/hadoop/tony_test/user_use_visit
file_name="$3"		# user_use_detail_m-201703
ftp_path="$4"		# /home/thadoop/tony_test/data

FTP_SERVER="10.200.65.50"
FTP_PORT="21"
FTP_USER="thadoop"
FTP_PWD="thadoop123!@#"

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

# 从hdfs下载文件到本地
echo "cmd:hdfs dfs -getmerge file"
hdfs dfs -getmerge ${hdfs_path}/* ${local_path}/.tmp.${file_name}

file_bytes=`wc -c ${local_path}/.tmp.${file_name} | awk -F " " {'print $1'}`
echo "The file '.tmp.${file_name}' size:${file_bytes}"

# 分割大文件
if [ ${file_bytes} -gt 1000 ]; then
  echo "cmd:split -a 6 -d -C 200M ${local_path}/.tmp.${file_name} ${local_path}/${file_name}-"
  split -a 6 -d -C 200M ${local_path}/.tmp.${file_name} ${local_path}/${file_name}-
  for subfile in ${local_path}/${file_name}*; do
    n=1
    seqnum=`echo ${subfile} | awk -F "-" -v n=$n '{printf("%06d",$NF+n)}'`
    mv ${subfile} ${local_path}/${file_name}-${seqnum}.dat
  done
else
   mv ${local_path}/.tmp.${file_name} ${local_path}/${file_name}-000001.dat
fi


# 生成校验文件信息:file_name|lines
for file in ${local_path}/${file_name}*; do
  data_file_name=`echo ${file} | awk -F"/" '{print $NF}'`
  lines=`wc -l ${file} | awk -F " " {'print $1'}`
  bytes=`wc -c ${file} | awk -F " " {'print $1'}`
  data_time=`echo ${file} | awk -F "-" {'print $2'}`
  echo "${data_file_name}|${bytes}|${lines}|${data_time}" 1>> ${local_path}/${file_name}.verf
done
echo "999999" 1>> ${local_path}/${file_name}.verf

echo "The verf file info:"
cat ${local_path}/${file_name}.verf


data_time=`echo ${file_name} | awk -F "-" {'print $2'}`
# 上传文件到ftp
ftp -n <<EOF 
open $FTP_SERVER $FTP_PORT
user $FTP_USER $FTP_PWD
prompt off
binary
mkdir $ftp_path/$data_time
cd $ftp_path/$data_time
lcd $local_path
mput ${file_name}*
close
bye
EOF

# 移动文件到备份路径,删除临时文件
mv ${local_path}/${file_name}* ${local_path}/bak/
rm -f ${local_path}/.tmp.${file_name}*
rm -f ${local_path}/..tmp.${file_name}*

if [[ $? -ne 0 ]]; then
  exit 1
fi


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
#!/bin/bash

if [[ $# -ne 4 ]]; then
  echo "Usage: $FUNCNAME from_hdfs_path to_temp_local_path file_name des_local_path"
  exit 1
fi

set -e

hdfs_path="$1"		    # /user/hive/warehouse/app.db/user_use_detail_sx0290_monthly/src_file_month=201703 
temp_local_path="$2"	# /mnt/cdmp_out/cdmp/ftp/gesheng/shaanxi/auto_fare/.temp
file_name="$3"		    # user_use_detail_m-201703
local_path="$4"		    # /mnt/cdmp_out/cdmp/ftp/gesheng/shaanxi/auto_fare

# 判断hdfs路径是否存在
if hdfs dfs -ls ${hdfs_path}/* 1> /dev/null 2>&1; then
    echo "HDFS file ${hdfs_path}/* exists"
else
    echo "HDFS file ${hdfs_path}/* does not exist"
    exit 1
fi

# 判断本地目标路径是否存在
if [ ! -d "${local_path}" ]; then
  umask 0022
  mkdir -p ${local_path}
fi

data_time=`echo ${file_name} | awk -F "-" {'print $2'}`
mkdir ${temp_local_path}/${data_time}			# /mnt/cdmp_out/cdmp/ftp/gesheng/shaanxi/auto_fare/.temp/201703
mkdir ${local_path}/${data_time}				# /mnt/cdmp_out/cdmp/ftp/gesheng/shaanxi/auto_fare/201703

# 从hdfs下载文件到本地
echo "cmd:hdfs dfs -getmerge file"
hdfs dfs -getmerge ${hdfs_path}/* ${temp_local_path}/${data_time}/.tmp.${file_name}

file_bytes=`wc -c ${temp_local_path}/${data_time}/.tmp.${file_name} | awk -F " " {'print $1'}`
echo "The file '.tmp.${file_name}' size:${file_bytes}"


# 分割大文件
if [ ${file_bytes} -gt 1000 ]; then
  echo "cmd:split -a 6 -d -C 200M ${temp_local_path}/${data_time}/.tmp.${file_name} ${temp_local_path}/${data_time}/${file_name}-"
  split -a 6 -d -C 200M ${temp_local_path}/${data_time}/.tmp.${file_name} ${temp_local_path}/${data_time}/${file_name}-
  for subfile in ${temp_local_path}/${data_time}/${file_name}*; do
    n=1
    seqnum=`echo ${subfile} | awk -F "-" -v n=$n '{printf("%06d",$NF+n)}'`
    mv ${subfile} ${temp_local_path}/${data_time}/${file_name}-${seqnum}.dat
  done
else
   mv ${temp_local_path}/${data_time}/.tmp.${file_name} ${temp_local_path}/${data_time}/${file_name}-000001.dat
fi


# 生成校验文件信息:file_name|bytes|lines|data_time
for file in ${temp_local_path}/${data_time}/${file_name}*; do
  data_file_name=`echo ${file} | awk -F"/" '{print $NF}'`
  lines=`wc -l ${file} | awk -F " " {'print $1'}`
  bytes=`wc -c ${file} | awk -F " " {'print $1'}`
  data_time=`echo ${file} | awk -F "-" {'print $2'}`
  echo "${data_file_name}|${bytes}|${lines}|${data_time}" 1>> ${temp_local_path}/${data_time}/${file_name}.verf
done
echo "999999" 1>> ${temp_local_path}/${data_time}/${file_name}.verf

echo "The verf file info:"
cat ${temp_local_path}/${data_time}/${file_name}.verf


# 移动文件到目的路径,删除临时文件
mv ${temp_local_path}/${data_time}/${file_name}* ${local_path}/${data_time}/
#rm -f ${temp_local_path}/${data_time}/.tmp.${file_name}*
#rm -f ${temp_local_path}/${data_time}/..tmp.${file_name}*

if [[ $? -ne 0 ]]; then
  exit 1
fi
