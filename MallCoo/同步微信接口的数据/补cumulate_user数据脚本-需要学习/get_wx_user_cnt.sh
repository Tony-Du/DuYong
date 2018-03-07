#!/bin/bash
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:/usr/local/bin:$PATH

dir=`dirname "$0"`

echo "" >> ${dir}/conf/new_insert.log
echo ===========`date +'%Y-%m-%d'` "修复数据"=========== >> ${dir}/conf/new_insert.log

function get_user_cnt()  #mallid day
{   
    echo $1,$2,$3
    mallid=$1
    day=$2
    wx_user=$3
	
    result=`curl -s --get http://op.service.mallcoo.cn/WX/WXApi/getAccessToken?mallID=$mallid`
    access_token=`echo $result|JSON.sh -b|grep -w d|awk -F " " '{print $2}'|tr -d '"'`
    
    cumulate_user_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusercumulate?access_token=${access_token}`
    
    usersummary_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusersummary?access_token=${access_token}`
    
    cumulate_user=`echo $cumulate_user_list|JSON.sh -b|grep cumulate_user|awk -F " " '{print $2}'|tr -d '"'`
    
    new_user=`echo $usersummary_list|JSON.sh -b|grep new_user|awk '{sum += $2} END{print sum}'`
    cancel_user=`echo $usersummary_list|JSON.sh -b|grep cancel_user|awk '{sum += $2} END{print sum}'`
    
    [[ -z $new_user ]] && new_user=0
    [[ -z $cancel_user ]] && cancel_user=0
    
    if [[ $new_user > 0 ]]; then 
        echo "${mallid},${new_user},${cancel_user},${cumulate_user}" >> $wx_user 
        echo "${date},${mallid},${new_user},${cancel_user},${cumulate_user}" >> ${dir}/conf/new_insert.log 
    fi
}


for date in `ls /locdata/wechat/user/`; do
    if [[ `cat "/locdata/wechat/user/$date/wx" |grep "^10414,.*,$" | wc -l` -ge 1 ]]; then
        sed -i '/^10414,.*,$/d' /locdata/wechat/user/$date/wx
        
		wx_user="/locdata/wechat/user/$date/wx"
        day=${date:0:4}-${date:4:2}-${date:6:2}
        get_user_cnt 10414 $day $wx_user
    fi
done



