#!/bin/bash
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:/usr/local/bin:$PATH

dir=`dirname "$0"`

echo "" >> ${dir}/conf/new_insert.log
echo ===========`date +'%Y-%m-%d'`=========== >> ${dir}/conf/new_insert.log
echo "" >> ${dir}/conf/wechat.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/wechat.log
echo "" >> ${dir}/conf/mall_wx_begin_date.log
echo ===========`date +'%Y-%m-%d'`=========== >> ${dir}/conf/mall_wx_begin_date.log

mallids=`hive -e "select cast(mallid as string) from mongo.mall where mallid >= 10000 and mallid <> 10002;"`

for mallid in mallids; do
    #mallid='10418'
    end_dt='2017-12-26'
    beg_dt='2014-12-01'

    save_dir="/apps/tony_test/wechat/$mallid"
    if [[ ! -d $save_dir ]];then
        mkdir -p $save_dir
    fi

    wx_user="/apps/tony_test/wechat/$mallid/wx"

    # get access_token
    result=`curl -s --get http://op.service.mallcoo.cn/WX/WXApi/getAccessToken?mallID=$mallid`
    access_token=`echo $result|JSON.sh -b|grep -w d|awk -F " " '{print $2}'|tr -d '"'`

    if [[ -z $access_token ]]; then
        isError=1
    else
        isError=0
    fi

    if ((isError==0)); then    
        end_s=`date -d "$end_dt" +%s`
        beg_s=`date -d "$beg_dt" +%s`
        
        flag=0
        
        while [ "$end_s" -ge "$beg_s" ]; do
            date=`date -d @"$end_s" +%Y%m%d`
            day=${date:0:4}-${date:4:2}-${date:6:2}
            
            if [[ $flag -ge 7 ]]; then
                echo $mallid,$date >> ${dir}/conf/mall_wx_begin_date.log
                break;
            fi
                                  
            cumulate_user_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusercumulate?access_token=${access_token}`

            usersummary_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusersummary?access_token=${access_token}`

            #cumulate_user=`echo $cumulate_user_list|JSON.sh -b|grep cumulate_user|awk -F " " '{print $2}'|tr -d '"'`
            cumulate_user=`echo $cumulate_user_list|JSON.sh -b|grep cumulate_user|awk '{sum += $2} END{print sum}'`

            new_user=`echo $usersummary_list|JSON.sh -b|grep new_user|awk '{sum += $2} END{print sum}'`
            
            cancel_user=`echo $usersummary_list|JSON.sh -b|grep cancel_user|awk '{sum += $2} END{print sum}'`
            
            [[ -z $new_user ]] && new_user=0        
            [[ -z $cancel_user ]] && cancel_user=0
            [[ -z $cumulate_user ]] && cumulate_user=0
                    
            #net_follow_count=$((new_user-cancel_user))
            if [ $new_user -gt 0 -o $cancel_user -ne 0 ]; then
                echo "${date},${mallid},${new_user},${cancel_user},${cumulate_user}" >> $wx_user 
                echo "${date},${mallid},${new_user},${cancel_user},${cumulate_user}" >> ${dir}/conf/new_insert.log 
            fi
            
            if [[ $cumulate_user -eq 0 ]]; then
                ((flag++))
            else
                flag=0
            fi
                    
            end_s=$((end_s-86400))
        done
    else
        echo `date +'%Y-%m-%d %H:%M:%S'` ERROR [$mallid] can not get access_token:${result}  >> ${dir}/conf/wechat.log
    fi

done
    