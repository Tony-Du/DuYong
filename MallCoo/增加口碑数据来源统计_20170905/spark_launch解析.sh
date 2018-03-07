

#!/bin/bash

dir=`dirname "$0"`    
#切换到 脚本 所在目录
# /apps/script/appspark/spark-launch.sh ,所以dir = /apps/script/appspark/

cd $dir
pyfiles=`ls -l |grep -E ".+(\.py)$"|awk '{print $9}'|xargs|tr ' ' ','`    #
echo $pyfiles

timestart=`date +"%Y-%m-%d %H:%M:%S"`        # 2017-09-11 00:00:01
date=`date -d yesterday +"%Y%m%d"`           # 20170910
tag01=`date -d yesterday +"%Y-%m-%d"`        # 2017-09-10
tag1=`date -d "-7 days" +"%Y%m%d"`           # 20170904
tag02=`date -d "-1 month" +"%Y-%m-"`         # 2017-08-
tag2=${date:0:6}01                           # 20170901


#tag2 取date = '20170907'前6位，再拼上'01',故得到'20170901'

echo "apptask begin!time now: ${timestart}">> $dir/log
#/bin/bash ${dir}/data_trans.sh
sh ${dir}/wanxiang.sh             #一点万象 数据 分析
# sh ${dir}/wxinterface.sh
# sh ${dir}/wxgraph.sh
# sh ${dir}/wechat.sh

for master in hadoop000 hadoop001 hadoop002; do
    num=$(ssh -p 33890 $master "`which jps`|grep '\bMaster\b'|wc -l")
    [[ $num -eq 0 ]] && ssh -p 33890 $master "/apps/spark/sbin/start-master.sh"
done

# ssh -p $port $user@$p 'cmd'  
   
# $port: ssh连接端口号  
# $user: ssh连接用户名  
# $ip: ssh连接的ip地址  
# cmd: 远程服务器需要执行的操作
# jps(Java Virtual Machine Process Status Tool)命令有个地方很不好，似乎只能显示当前用户的java进程


date="${date}" 
tag01="${tag01}" 
tag1="${tag1}" 
tag02="${tag02}" 
tag2="${tag2}" 
/apps/spark/bin/spark-submit --master spark://hadoop000:7077,hadoop001:7077,hadoop002:7077 
                             --conf spark.buffer.pageSize=4m 
							 --conf spark.shuffle.memoryFraction=0.3 
							 --conf spark.shuffle.safetyFraction=0.9 
							 --py-files $pyfiles 
							 --jars /root/day_mq/udf3_mq.jar,/root/feihu/udf4.jar,/root/location/date2date.jar 
							 --driver-memory 3g ${dir}/app-start.py

# hive 版本监控 
/bin/bash /apps/script/monitor/hive_version.sh
