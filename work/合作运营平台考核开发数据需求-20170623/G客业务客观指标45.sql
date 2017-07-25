insert overwrite table qushupingtai.qspt_hzyykh_17_assess_4 partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,(t.assess_4_cnt/t.assess_4_user_num)/t.assess_4_pr_num as prog_watch_avg_cnt 
  from (
        select p.business_id   
              ,count(distinct a.usernum_id) as assess_4_user_num  
              ,count(1) as assess_4_cnt                  
              ,count(distinct p.program_id) assess_4_pr_num 
          from rptdata.fact_use_detail a
          join ${GK_PROG_ID_TBL} p
            on a.program_id = p.program_id
         where a.src_file_day >= '${MONTH_START_DAY}' 
           and a.src_file_day <= '${MONTH_END_DAY}'
         group by p.business_id 
       ) t;


insert overwrite table qushupingtai.qspt_hzyykh_17_assess_5 partition (src_file_month = '${SRC_FILE_MONTH}')
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,(t.assess_5_duration_sec/t.assess_5_user_num)/t.assess_5_pr_num as prog_watch_avg_dur_sec
  from (
        select p.business_id  
              ,count(distinct a.usernum_id) as assess_5_user_num 
              ,sum(a.duration_sec) as assess_5_duration_sec 
              ,count(distinct p.program_id) as assess_5_pr_num 
          from rptdata.fact_use_detail a
          join ${GK_PROG_ID_TBL} p
            on a.program_id = p.program_id
         where a.src_file_day >= '${MONTH_START_DAY}' 
           and a.src_file_day <= '${MONTH_END_DAY}'
         group by p.business_id 
       ) t;

 

${OUTPUT_RESULT}
select t.statis_month
      ,t.business_id
      ,sum(t.prog_watch_avg_cnt) as prog_watch_avg_cnt
      ,sum(t.prog_watch_avg_dur_sec) as prog_watch_avg_dur_sec
  from ( 
        select a4.statis_month
              ,a4.business_id
              ,a4.prog_watch_avg_cnt
              ,0 as prog_watch_avg_dur_sec 
          from qushupingtai.qspt_hzyykh_17_assess_4 a4
         where a4.src_file_month = '${SRC_FILE_MONTH}'  
          
         union all
         
        select a5.statis_month
              ,a5.business_id
              ,0 as prog_watch_avg_cnt
              ,a5.prog_watch_avg_dur_sec 
          from qushupingtai.qspt_hzyykh_17_assess_5 a5
         where a5.src_file_month = '${SRC_FILE_MONTH}'  
       ) t         
 group by t.statis_month,t.business_id
   