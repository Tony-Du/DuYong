
--oracle dmt层建表如下(废除)

create table cdmp_dmt.fact_cdn_flow_daily 
(
	statis_day          VARCHAR2(8) NOT NULL ,
	metric_cat_name 	NVARCHAR2(50) NOT NULL ,
	metric_name         NVARCHAR2(100) NOT NULL ,
	metric_value        NUMBER(28, 2) NOT NULL 
);

--oracle 修改表名，添加注释
CREATE TABLE cdmp_dmt.fact_high_level_metric_daily
(
    statis_day           VARCHAR2(8) NOT NULL ,
    metric_cat_name      NVARCHAR2(50) NOT NULL ,
    metric_name          NVARCHAR2(100) NOT NULL ,
    metric_value         NUMBER NULL ,
    dw_crt_by            VARCHAR2(50) NULL , --标注该条记录是由哪个ETL程序抽的，有可能有一部分需要手工从CDMP历史指标中抽取
    dw_crt_at            VARCHAR2(S) NULL , --标注该条记录的创建时间
    dw_del_flag          VARCHAR2(1) NULL    --记录是否逻辑删除（insert时默认值为n,删除记录置为y,也就是不物理删除记录），ETL重跑时先要逻辑删除上次的记录
);
 
COMMENT ON TABLE fact_high_level_metric_daily IS '高层指标按日统计表';
 
COMMENT ON COLUMN fact_high_level_metric_daily.statis_day IS '统计日期';
 
COMMENT ON COLUMN fact_high_level_metric_daily.metric_cat_name IS '指标类别，如彩信日报、流量日报';
 
COMMENT ON COLUMN fact_high_level_metric_daily.metric_name IS '指标名称，命名规则：
<指标维度>+[指标前缀]+<基本指标>，具体见 - http://192.168.200.213:8090/pages/viewpage.action?pageId=1870436';
 
COMMENT ON COLUMN fact_high_level_metric_daily.metric_value IS '指标值';




--第四版  模型变更

--hive 视图如下：

create view rpt.fact_cdn_flow_daily_v as 

with flow_stat as 
(
 select a.src_file_day
       ,a.net_type_id
       ,a.cdn_id
       ,a.flow_kb
       ,b.belong_type_id
       ,b.broadcast_type_id
       ,c.term_video_type_id
       ,c.term_video_soft_id
   from rptdata.fact_use_detail a
   left join msc.dim_cdn_domain_ex b 
     on a.service_url_domain = b.service_url_domain
   left join rpt.dim_term_prod_v c
     on a.termprod_id = c.term_prod_id
where a.src_file_day = '20170501'     --'${SRC_FILE_DAY}'
)
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '基地', '总流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where belong_type_id = '00001'
   and broadcast_type_id <> '-998'
 group by src_file_day

 union all
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', case when net_type_id in ('1', '2', '5') then '移动' else '非移动' end, '数据流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 group by src_file_day
         ,case when net_type_id in ('1', '2', '5') then '移动' else '非移动' end

 union all 
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '移动4G', '数据流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where net_type_id = '5'
 group by src_file_day
 
 union all 
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '和视频', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where term_video_type_id = 'TV00001'
 group by src_file_day
 
 union all 
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '和视界', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where term_video_type_id = 'TV00002'
 group by src_file_day
 
 union all 
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '直播', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where broadcast_type_id = '12'
 group by src_file_day
 
 union all 
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '能力输出', '总流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where term_video_soft_id in ('TST000003', 'TST000004')
 group by src_file_day

 union all 
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', 'SDK', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where term_video_soft_id = 'TST000003'
 group by src_file_day 

 union all 
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', 'OPENAPI', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where term_video_soft_id = 'TST000004'
 group by src_file_day 

 union all 
 
select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '第三方', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where cdn_id in ('4000', '4001')
 group by src_file_day  

 union all 

select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '自建CDN', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where cdn_id in ('1000', '2000')
 group by src_file_day  

 union all 

select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '咪咕视频', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where term_video_type_id = 'TV00303'
 group by src_file_day 

  union all 

select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '咪咕影院', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where term_video_type_id = 'TV00203'
 group by src_file_day

 union all 

select src_file_day as statis_day
      ,'流量日报' as metric_cat_name
      ,concat_ws('_', '日', '咪咕直播', '流量') as metric_name
      ,sum(flow_kb)/1024/1024/1024 as metric_value
  from flow_stat
 where term_video_type_id = 'TV00306'
 group by src_file_day;
 
 
 