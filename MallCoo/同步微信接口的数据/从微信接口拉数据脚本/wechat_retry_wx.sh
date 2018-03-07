#!/bin/bash
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:/usr/local/bin:$PATH

dir=`dirname "$0"`

appkey=0732025a51fbe3abe7b6bda0e00a4e10
secret=8c29609ac2ea486ca4deec298474aff5

#dates="20170726 20170725 20170724 20170723 20170722 20170721 20170720 20170719 20170718 20170717"

mallids=`hive -e "select concat(m.mallid,'-',if(m.mallcootype='sp', 1, 0),',',min(w.date)) 
                    from mongo.mall m 
                   inner join mq.wx_appcount w 
                      on m.mallid=w.mallid 
                   where w.new_user>0 
                     and m.mallid > 10000
                   group by m.mallid,'-',if(m.mallcootype='sp', 1, 0),',';"`

echo "" >> ${dir}/conf/wechat.log
echo ===========`date +'%Y-%m-%d'`=========== >> ${dir}/conf/wechat.log

echo "" >> ${dir}/conf/new_insert.log
echo ===========`date +'%Y-%m-%d'`=========== >> ${dir}/conf/new_insert.log
echo "" >> ${dir}/conf/mall_wx_begin_date.log
echo ===========`date +'%Y-%m-%d'`=========== >> ${dir}/conf/mall_wx_begin_date.log

for mall in $mallids; do

    mallno=`echo $mall | cut -d ',' -f 1`
    end_dt=`echo $mall | cut -d ',' -f 2`
    mallid=`echo $mallno | cut -d '-' -f 1`
    isSP=`echo $mallno | cut -d '-' -f 2`
    if [[ -z $end_dt ]]; then
        end_dt="20171217"
    fi 
    #mall="42-0"
    
    zero_day=0

    if [[ $isSP -eq 1 ]]; then

        result=`curl -s --get http://op.service.mallcoo.cn/WX/WXApi/getAccessToken?mallID=$mallid`
        access_token=`echo $result|JSON.sh -b|grep -w d|awk -F " " '{print $2}'|tr -d '"'`

        if [[ -z $access_token ]]; then
            isError=1
        else
            isError=0
        fi
    else
        ts=`date +%s`
        sig=`echo -n "appkey=${appkey}&mallID=${mallid}&ts=${ts}&${secret}" |openssl md5|cut -d ' ' -f 2|tr '[a-z]' '[A-Z]'`
        result=`curl -s -k --get --data "appkey=${appkey}&ts=${ts}&sig=${sig}&mallID=${mallid}" http://openapi.mallcoo.cn/Weixin/GetAccessTokenByMallID`

        access_token=`echo $result|JSON.sh -b|grep access_token|awk -F " " '{print $2}'|tr -d '"'`

        if [[ `echo $result|JSON.sh -b|grep errCode|awk -F " " '{print $2}'|tr -d '"'` -eq 0 ]]; then
            isError=0
        else
            isError=1
        fi
    fi
    if ((isError==0)); then
        #loop "2017-07-30" to "2017-04-01"
        
        #weixi data only select data>="2014-12-01" 
        beg_dt='2014-12-01'
        end_s=`date -d "$end_dt" +%s`
        beg_s=`date -d "$beg_dt" +%s`

        while [ "$end_s" -ge "$beg_s" ]; do

            date=`date -d @"$end_s" +%Y%m%d`
            day=${date:0:4}-${date:4:2}-${date:6:2}
               
            # if new_user=0 day >=21 exit
            if [[ $zero_day -ge 7 ]]; then
                echo $mallid,$date  >> ${dir}/conf/mall_wx_begin_date.log
                break;
            fi

            mkdir -p /locdata/wechat/user/$date
            wx_user="/locdata/wechat/user/$date/wx"
            
            #if ((isError==0)); then
            mallid_exists=0
            if [ -f "$wx_user" ]; then
                mallid_exists=`cat $wx_user |cut -d ',' -f 1 |grep -w $mallid`                
            fi 
            cumulate_user=0
            if [[ -z $mallid_exists  ]]; then 
                #echo `date +'%Y-%m-%d %H:%M:%S'` INFO [$mallid] access_token:${result} >> ${dir}/conf/wechat.log
                cumulate_user_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusercumulate?access_token=${access_token}`

                usersummary_list=`curl -s -d "{\"begin_date\": \"${day}\", \"end_date\": \"${day}\"}" https://api.weixin.qq.com/datacube/getusersummary?access_token=${access_token}`

                cumulate_user=`echo $cumulate_user_list|JSON.sh -b|grep cumulate_user|awk -F " " '{print $2}'|tr -d '"'`
                new_user=
                cancel_user=

                new_user=`echo $usersummary_list|JSON.sh -b|grep new_user|awk '{sum += $2} END{print sum}'`
                cancel_user=`echo $usersummary_list|JSON.sh -b|grep cancel_user|awk '{sum += $2} END{print sum}'`

                [[ -z $new_user ]] && new_user=0
                [[ -z $cancel_user ]] && cancel_user=0
                net_follow_count=$((new_user-cancel_user))

                if [[ $new_user > 0 ]]; then 
                    echo "${mallid},${new_user},${cancel_user},${cumulate_user}" >> $wx_user 
                    echo "${date},${mallid},${new_user},${cancel_user},${cumulate_user}" >> ${dir}/conf/new_insert.log 
                fi 

            fi
            if [[ $cumulate_user -eq 0 ]]; then
                ((zero_day++));
            else
                zero_day=0    
            fi
            end_s=$((end_s-86400))

        done
    else
        echo `date +'%Y-%m-%d %H:%M:%S'` ERROR [$mallid] can not get access_token:${result}  >> ${dir}/conf/wechat.log
    fi

done
