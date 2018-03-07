#!/bin/bash
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:/usr/local/bin:$PATH


# /apps/script/appspark/
dir=`dirname "$0"`

echo "" >> ${dir}/conf/new_insert.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/new_insert.log
echo "" >> ${dir}/conf/wechat.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/wechat.log


function get_user_cnt()  
{   
    echo $1,$2,$3,$4
    mallid=$1
    isSP=$2
    day=$3
    wx_user=$4
    
    
    if [[ $isSP -eq 1 ]]; then 
    
        result=`curl -s --get http://op.service.mallcoo.cn/WX/WXApi/getAccessToken?mallID=$mallid`
        echo $result >> ${dir}/conf/wechat.log
        
        access_token=`echo $result|JSON.sh -b|grep -w d|awk -F " " '{print $2}'|tr -d '"'`
        
        if [[ -z $access_token ]]; then 
            isError=1
        else
            isError=0
        fi
    else
        appkey=0732025a51fbe3abe7b6bda0e00a4e10
        secret=8c29609ac2ea486ca4deec298474aff5
        ts=`date +%s`
        sig=`echo -n "appkey=${appkey}&mallID=${mallid}&ts=${ts}&${secret}" |openssl md5|cut -d ' ' -f 2|tr '[a-z]' '[A-Z]'`
        
        result=`curl -s -k --get --data "appkey=${appkey}&ts=${ts}&sig=${sig}&mallID=${mallid}" http://openapi.mallcoo.cn/Weixin/GetAccessTokenByMallID`
        echo $result >> ${dir}/conf/wechat.log
        
        access_token=`echo $result|JSON.sh -b|grep access_token|awk -F " " '{print $2}'|tr -d '"'`
    
        if [[ `echo $result | JSON.sh -b | grep errCode | awk -F " " '{print $2}' | tr -d '"'` -eq 0 ]]; then
            isError=0
        else
            isError=1
        fi
    fi
    
    if ((isError==0)); then
    
        cumulate_user_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusercumulate?access_token=${access_token}`        
        usersummary_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusersummary?access_token=${access_token}`       
        
        cumulate_user=`echo $cumulate_user_list|JSON.sh -b|grep cumulate_user|awk -F " " '{print $2}'|tr -d '"'`        
        new_user=`echo $usersummary_list|JSON.sh -b|grep new_user|awk '{sum += $2} END{print sum}'`
        cancel_user=`echo $usersummary_list|JSON.sh -b|grep cancel_user|awk '{sum += $2} END{print sum}'`
        
        [[ -z $new_user ]] && new_user=0
        [[ -z $cancel_user ]] && cancel_user=0
        [[ -z $cumulate_user ]] && cumulate_user=0
        
        if [ $new_user -gt 0 -o $cancel_user -ne 0 ]; then
            echo "${mallid},${new_user},${cancel_user},${cumulate_user}" >> $wx_user 
            echo "${date},${mallid},${new_user},${cancel_user},${cumulate_user}" >> ${dir}/conf/new_insert.log 
        fi
    else
        echo `date +'%Y-%m-%d %H:%M:%S'` ERROR [$mallid] can not get access_token:${result}  >> ${dir}/conf/wechat.log
    fi
}


mallid='10218'
date='20180103'
day=${date:0:4}-${date:4:2}-${date:6:2}
wx_user="/locdata/wechat/user/$date/wx"
isSP=1

get_user_cnt $mallid $isSP $day $wx_user