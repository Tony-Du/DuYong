#!/bin/bash
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:/usr/local/bin:$PATH

# /apps/tony_test
dir=`dirname "$0"`

echo "" >> ${dir}/conf/new_insert.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/new_insert.log
echo "" >> ${dir}/conf/wechat.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/wechat.log

mallids=(10345 10348 10278 10183 10509 10114)

for mallid in $mallids; do
    
    echo 'mallid is ' $mallid >> ${dir}/conf/wechat.log

    # get access_token
    result=`curl -s --get http://op.service.mallcoo.cn/WX/WXApi/getAccessToken?mallID=$mallid`
    access_token=`echo $result|JSON.sh -b|grep -w d|awk -F " " '{print $2}'|tr -d '"'`
    echo 'access_token is ' $access_token >> ${dir}/conf/wechat.log

    if [[ -z $access_token ]]; then
        isError=1
    else
        isError=0
    fi

    if ((isError==0)); then

        for date in `ls /locdata/wechat/user/`; do
            
            echo 'date is ' $date >> ${dir}/conf/wechat.log

            day=${date:0:4}-${date:4:2}-${date:6:2}
            wx_user="/locdata/wechat/user/$date/wx"

            mallid_exists=0
            if [ -f $wx_user ]; then
                mallid_exists=`cat $wx_user | cut -d ',' -f 1 | grep -w $mallid`
            fi
            
            echo 'mallid_exists flag is ' $mallid_exists >> ${dir}/conf/wechat.log
            

            if [[ -z $mallid_exists ]]; then

                cumulate_user_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusercumulate?access_token=${access_token}`
                echo 'cumulate_user_list : ' $cumulate_user_list >> ${dir}/conf/wechat.log

                usersummary_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusersummary?access_token=${access_token}`
                echo 'usersummary_list : ' $usersummary_list >> ${dir}/conf/wechat.log

                #cumulate_user=`echo $cumulate_user_list|JSON.sh -b|grep cumulate_user|awk -F " " '{print $2}'|tr -d '"'`
                cumulate_user=`echo $cumulate_user_list|JSON.sh -b|grep cumulate_user|awk '{sum += $2} END{print sum}'`
                new_user=`echo $usersummary_list|JSON.sh -b|grep new_user|awk '{sum += $2} END{print sum}'`
                cancel_user=`echo $usersummary_list|JSON.sh -b|grep cancel_user|awk '{sum += $2} END{print sum}'`

                [[ -z $new_user ]] && new_user=0
                [[ -z $cancel_user ]] && cancel_user=0
                [[ -z $cumulate_user ]] && cumulate_user=0

                #net_follow_count=$((new_user-cancel_user))
                if [ $new_user -gt 0 -o $cancel_user -ne 0 ]; then
                    echo "${mallid},${new_user},${cancel_user},${cumulate_user}" >> $wx_user
                    echo "${date},${mallid},${new_user},${cancel_user},${cumulate_user}" >> ${dir}/conf/new_insert.log
                fi

            fi

        done

    else
        echo `date +'%Y-%m-%d %H:%M:%S'` ERROR [$mallid] can not get access_token:${result}  >> ${dir}/conf/wechat.log
    fi

done
