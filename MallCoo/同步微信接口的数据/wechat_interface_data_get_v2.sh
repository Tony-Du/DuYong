#!/bin/bash
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:/usr/local/bin:$PATH

# /apps/tony_test
dir=`dirname "$0"`

echo "" >> ${dir}/conf/new_insert.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/new_insert.log
echo "" >> ${dir}/conf/wechat.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/wechat.log
echo "" >> ${dir}/conf/mall_wx_begin_date.log
echo ===========`date +'%Y-%m-%d'`=========== >> ${dir}/conf/mall_wx_begin_date.log


# mallid='10420'
# mallcootype: sp pp
mallids=`hive -e "select concat(mallid, '-', if(mallcootype='sp',1,0)) from mongo.mall where mallid >= 10000 and mallid <> 10002; "`
end_dt='2017-12-26'
beg_dt='2014-12-01'

appkey=0732025a51fbe3abe7b6bda0e00a4e10
secret=8c29609ac2ea486ca4deec298474aff5

for mall in $mallids; do

    mallid=`echo $mall | cut -d '-' -f 1`    # 306 
    isSP=`echo $mall | cut -d '-' -f 2`      # 0 
    
    
    save_dir="/apps/tony_test/wechat/$mallid"
    if [[ ! -d $save_dir ]];then
        mkdir -p $save_dir
        mallid_exists=0   # not exists
    else 
        mallid_exists=1
    fi
    wx_user="/apps/tony_test/wechat/$mallid/wx"

    
    if [[ $mallid_exists -eq 0 ]]; then
    
        if [[ $isSp -eq 1 ]]; then 
        
            # get access_token
            result=`curl -s --get http://op.service.mallcoo.cn/WX/WXApi/getAccessToken?mallID=$mallid`
			echo $result >> ${dir}/conf/wechat.log
			
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
            end_s=`date -d "$end_dt" +%s`
            beg_s=`date -d "$beg_dt" +%s`
            
            flag=0
            
            while [ "$end_s" -ge "$beg_s" ]; do
                date=`date -d @"$end_s" +%Y%m%d`
                day=${date:0:4}-${date:4:2}-${date:6:2}
                
                if [[ $flag -ge 10 ]]; then
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
                
                # 24*60*60				
                end_s=$((end_s-86400))
            done
        else
            echo `date +'%Y-%m-%d %H:%M:%S'` ERROR [$mallid] can not get access_token:${result}  >> ${dir}/conf/wechat.log
        fi
    
    fi
done
    