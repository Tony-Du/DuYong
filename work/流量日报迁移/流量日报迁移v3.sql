

with flow_stat as 
(
 select a.src_file_day
       ,a.net_type_id
       ,a.cdn_id
       ,a.broadcast_type_id
       ,a.flow_kb
       ,c.term_video_type_id
       ,c.term_video_soft_id
   from rptdata.fact_use_detail a
   left join rpt.dim_term_prod_v c
     on a.termprod_id = c.term_prod_id
where a.src_file_day = '20170501'     --'${SRC_FILE_DAY}'
)
select statis_day
      ,metric_cat_name
      ,metric_name
      ,sum(metric_value)/1024/1024/1024 as metric_value
      ,'etl' as dw_crt_by
      ,from_unixtime(unix_timestamp(), 'yyyyMMddHHmmss') as dw_crt_at
      ,'N' as dw_del_flag
  from (
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '����', '������') as metric_name
              ,0 as metric_value
          from flow_stat

         union all
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', case when net_type_id in ('1', '2', '5') then '�ƶ�' else '���ƶ�' end, '��������') as metric_name
              ,flow_kb as metric_value
          from flow_stat

         union all 
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '�ƶ�4G', '��������') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where net_type_id = '5'
         
         union all 
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '����Ƶ', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where term_video_type_id = 'TV00001'

         union all 
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '���ӽ�', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where term_video_type_id = 'TV00002'
         
         union all 
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', 'ֱ��', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where broadcast_type_id = '12'
         
         union all 
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '�������', '������') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where term_video_soft_id in ('TST000003', 'TST000004')

         union all 
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', 'SDK', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where term_video_soft_id = 'TST000003'

         union all 
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', 'OPENAPI', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where term_video_soft_id = 'TST000004'

         union all 
         
        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '������', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where cdn_id in ('4000', '4001') 

         union all 

        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '�Խ�CDN', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where cdn_id in ('1000', '2000')

         union all 

        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '�乾��Ƶ', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where term_video_type_id = 'TV00303'

          union all 

        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '�乾ӰԺ', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where term_video_type_id = 'TV00203'

         union all 

        select src_file_day as statis_day
              ,'�����ձ�' as metric_cat_name
              ,concat_ws('_', '��', '�乾ֱ��', '����') as metric_name
              ,flow_kb as metric_value
          from flow_stat
         where term_video_type_id = 'TV00306'
       ) t
 group by statis_day, metric_cat_name, metric_name;

