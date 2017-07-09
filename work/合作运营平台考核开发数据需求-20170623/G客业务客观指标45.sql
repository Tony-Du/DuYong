insert overwrite table cdmpview.qspt_hzyykh_05_Assess_4 
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,(t.assess_4_cnt/t.assess_4_user_num)/t.assess_4_pr_num as prog_watch_avg_cnt 
  from (
        select p.program_id as business_id   
              ,count(distinct a.usernum_id) as assess_4_user_num  
              ,count(1) as assess_4_cnt                  
              ,count(distinct p.business_id) assess_4_pr_num 
          from rptdata.fact_use_detail a
          join cdmpview.tmp_wsj_Assess_4_program p
            on a.program_id = p.business_id
         where a.src_file_day >= '${MONTH_START_DAY}}' 
           and a.src_file_day <= '${MONTH_END_DAY}}'
         group by p.program_id 
       ) t;


insert overwrite table cdmpview.qspt_hzyykh_05_Assess_5 
select substr('${MONTH_START_DAY}', 1, 6) as statis_month
      ,t.business_id
      ,(t.assess_5_duration_sec/t.assess_5_user_num)/t.assess_5_pr_num as prog_watch_avg_dur_sec
  from (
        select p.program_id as business_id  
              ,count(distinct a.usernum_id) as assess_5_user_num 
              ,sum(a.duration_sec) as assess_5_duration_sec 
              ,count(distinct p.business_id) as assess_5_pr_num 
          from rptdata.fact_use_detail a
          join cdmpview.tmp_wsj_Assess_4_program p
            on a.program_id = p.business_id
         where a.src_file_day >= '${MONTH_START_DAY}}' 
           and a.src_file_day <= '${MONTH_END_DAY}}'
         group by p.program_id 
       ) t;


${OUTPUT_RESULT}
select nvl(a4.statis_month, a5.statis_month) as statis_month
      ,nvl(a4.business_id, a5.business_id) as business_id
      ,nvl(a4.prog_watch_avg_cnt, 0) as prog_watch_avg_cnt
      ,nvl(a5.prog_watch_avg_dur_sec, 0) as prog_watch_avg_dur_sec
  from cdmpview.qspt_hzyykh_05_Assess_4 a4  
  full join cdmpview.qspt_hzyykh_05_Assess_5 a5
    on a4.business_id = a5.business_id;
  