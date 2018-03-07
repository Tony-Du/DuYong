#!/bin/bash
export PATH=/apps/jdk/bin:/apps/hadoop/sbin:/apps/hadoop/bin:/apps/hive/bin:/usr/local/bin:$PATH

# /apps/tony_test
dir=`dirname "$0"`

echo "" >> ${dir}/conf/new_insert.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/new_insert.log
echo "" >> ${dir}/conf/wechat.log
echo ==========`date +'%Y-%m-%d'`========== >> ${dir}/conf/wechat.log

mallids=`hive -e "select mallid from mongo.mall where mallid in (10345,10348,10278,10183,10509,10114);"`
des_dir="/locdata/wechat/user"

for mallid in $mallids; do

    echo '======= mallid=' $mallid ' =========' >> ${dir}/conf/wechat.log
    
    src_file="/apps/tony_test/wechat/$mallid/wx"
    
    for date in `ls $des_dir`; do
    
        echo 'date : ' $date >> ${dir}/conf/wechat.log
        
        #get data from my local path
        record=`cat $src_file | grep "^$date,$mallid,.*$"`
        echo 'record (from src_file) : ' $record >> ${dir}/conf/wechat.log
        
        new_user=`echo $record | cut -d ',' -f 3`
        cancel_user=`echo $record | cut -d ',' -f 4`
        cumulate_user=`echo $record | cut -d ',' -f 5`
        
        
        #write data into des_file       
        des_file="/locdata/wechat/user/$date/wx"
        
        mallid_exists=''
        if [ -f $des_file ]; then
            mallid_exists=`cat $des_file | cut -d ',' -f 1 | grep -w $mallid`
        fi      
        echo 'mallid_exists is ' $mallid_exists >> ${dir}/conf/wechat.log
        
        if [[ -z $mallid_exists ]]; then        
        
            [[ -z $new_user ]] && new_user=0
            [[ -z $cancel_user ]] && cancel_user=0
            [[ -z $cumulate_user ]] && cumulate_user=0
            
            if [ $new_user -gt 0 -o $cancel_user -ne 0 ]; then
                #echo "${mallid},${new_user},${cancel_user},${cumulate_user}" >> $des_file
                echo "${date},${mallid},${new_user},${cancel_user},${cumulate_user}" >> ${dir}/conf/wechat.log
            fi
            echo "--------------------" >> ${dir}/conf/wechat.log
            
        fi
            
    done
    
done
