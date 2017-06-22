#!bin/bash

##################################
# CPA 渠道配置文件接口文件
# 输入参数:<数据周期YYYYMMDD>
#################################




# 检查参数
if [ $# -ne 4 ]
   then echo "The number of parameters must be four"
        exit 1
fi

# 安全性
set -e

# 设置变量
local_path="$1"
file_name="$2"
src_file_day="$3"
ftp_path="$4"

FTP_SERVER="172.20.37.35"
FTP_PORT="21"
FTP_USER="cpa013"
FTP_PWD="migucpa#013vd"

# HQL
hql="select substr('${src_file_day}',1,6) as stat_month  
           ,'698040' as migu_company_code 
           ,a.term_video_type_id as product_id 
           ,a.term_video_type_name as product_name 
           ,case when a.term_os_type_id = '00' then '0' 
                 when a.term_os_type_id = '04' then '1'
                 when a.term_os_type_id = '05' then '1'
                 when a.term_os_type_id = '06' then '0'
                 when a.term_os_type_id = '09' then '0' else '' end as product_type 
           ,'' as start_date
           ,'' as expire_date 
       from rptdata.dim_term_prod a 
      group by a.term_video_type_id
              ,a.term_video_type_name
              ,case when a.term_os_type_id = '00' then '0' 
                    when a.term_os_type_id = '04' then '1'
                    when a.term_os_type_id = '05' then '1'
                    when a.term_os_type_id = '06' then '0'
                    when a.term_os_type_id = '09' then '0' else '' end;
    "

# 检查路径
if [ ! -d "${local_path}" ]
then umask 0022                 
     mkdir -p ${local_path}     
fi

# 清理旧文件
if [ ! -f "${local_path}/.tmp.${file_name}" ]
then rm -f ${local_path}/.tmp.${file_name}
fi

rm -f ${local_path}/${file_name}

# 执行HQL
echo "cmd: hive -e 'hql' "
hive -e "${hql}" | sed 's/\t/|/g' > ${local_path}/.tmp.${file_name}


# 移动临时文件
mv ${local_path}/.tmp.${file_name} ${local_path}/${file_name}.0000

# 生成校验文件
data_file_name=`echo ${local_path}/${file_name}* | awk -F'/' '{print $NF}'`
lines=`wc -l ${local_path}/${file_name}* | awk -F' ' '{print $1}'`
echo "${data_file_name}|${lines}" 1>> ${local_path}/${file_name}.9999
echo "The verify file infomation:"
cat ${local_path}/${file_name}.9999

# 压缩文件
echo "cmd: gzip"
gzip ${local_path}/${file_name}*

# 上传文件到ftp
ftp -n <<EOF
open $FTP_SERVER
user $FTP_USER $FTP_PWD
passive
binary
cd $ftp_path
lcd $local_path                          
mput ${file_name}*
close
bye
EOF

# 移动文件到备份路径
mv ${local_path}/${file_name}* ${local_path}/bak

# 正确退出
if [[ $? -ne 0 ]]; then
  exit 1
fi

