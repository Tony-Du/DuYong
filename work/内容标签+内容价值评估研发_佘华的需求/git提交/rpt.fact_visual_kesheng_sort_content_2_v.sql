create or replace view rpt.fact_visual_kesheng_sort_content_2_v as
select a.src_file_day                                                --日期
      ,nvl(p.prov_name, '-998') as prov_name                         --省份
      ,nvl(d1.term_video_type_name, '-998') as term_video_type_name  --终端大类名称
      ,a.content_id                                                  --内容ID
      ,nvl(d2.content_name, '-998') as content_name                  --内容名称
      ,nvl(d2.ori_publish, '-998') as ori_publish                    --原创发行
      ,nvl(d2.authorization_way, '-998') as authorization_way        --授权方式
      ,nvl(d2.migu_publish, '-998') as migu_publish                  --咪咕发行(首发状态)
      ,nvl(d3.copyright_type, '-998') as copyright_typev             --版权类型
      ,nvl(d3.ncp_id, '-998') as ncp_id                              --NCP_ID
      ,nvl(d4.con_class_1_name, '-998') as con_class_1_name          --一级分类名称
      ,nvl(d4.con_class_2_name, '-998')  as con_class_2_name         --二级分类名称    
      ,case when d5.chrgprod_price is null then '-998' 
            when d5.chrgprod_price > 0.1 then '计费产品' else '免费产品' end as fee_type      
      ,count(distinct a.session) as use_cnt                          --使用次数
  from rptdata.fact_user_play_detail a
  left join rpt.dim_term_prod_v d1              --终端产品维表
    on a.term_prod_id = d1.term_prod_id
  left join rptdata.dim_video_content d2        --内容维表
    on a.content_id = d2.content_id
  left join rptdata.dim_content_copyright d3    --内容版权维表
    on d2.copyright_provider = d3.copyright_id
  left join rptdata.dim_visual_content_class_new_d d4   --内容分类维表
    on a.content_id = d4.content_id
  left join rptdata.dim_charge_product d5       --计费产品维表
    on a.product_id = d5.chrgprod_id
  left join (select distinct prov_name, prov_id from rptdata.dim_region) p
    on a.ip_province_id = p.prov_id
 --where a.src_file_day <= from_unixtime(unix_timestamp(),'yyyyMMdd') 
   --and a.src_file_day > from_unixtime(unix_timestamp() - 14*24*60*60, 'yyyyMMdd')
 group by src_file_day
         ,prov_name
         ,term_video_type_name
         ,a.content_id         
         ,content_name      
         ,ori_publish       
         ,authorization_way 
         ,migu_publish      
         ,copyright_type    
         ,ncp_id            
         ,con_class_1_name
         ,con_class_2_name
         ,case when d5.chrgprod_price is null then '-998' 
               when d5.chrgprod_price > 0.1 then '计费产品' else '免费产品' end;