

1.没有分区,且字段一一对应

-- oracle中的表

create table TEST_DY
(
product_key    VARCHAR2(8),
product_name   VARCHAR2(12),
app_ver_code   VARCHAR2(12),
app_channel_id VARCHAR2(20),
event_name     VARCHAR2(32),
param_name     VARCHAR2(32),
param_val      VARCHAR2(64),
val_cnt        NUMBER(32),
val_pct        NUMBER(8,4),
src_file_day   CHAR(8)
);

--hive中的表
create table temp.cpa_event_params_daily_dy (
product_key    string,
product_name   string,
app_ver_code   string,
app_channel_id string,
event_name     string,
param_name     string,
param_val      string,
val_cnt        bigint,
val_pct        decimal(8,4),
src_file_day   string
);

select * from temp.cpa_event_params_daily_dy;

sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table TEST_DY \
--export-dir /user/hive/warehouse/temp.db/cpa_event_params_daily_dy \
--columns product_key,product_name,app_ver_code,app_channel_id,event_name,param_name,param_val,val_cnt,val_pct,src_file_day \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' 
结果：导出成功，但是出现中文编码问题（oracle：GBK，hive：UTF-8）


2.没有分区,但字段不对应

-- oracle中的表
create table TEST2_DY
(
src_file_day   VARCHAR(64),
product_key    VARCHAR2(64),
product_name   VARCHAR2(64),
app_ver_code   VARCHAR2(64),
app_channel_id VARCHAR2(64),
event_name     VARCHAR2(64),
param_name     VARCHAR2(64),
param_val      NUMBER(64)
val_cnt        NUMBER(8,4),
val_pct       VARCHAR2(64),
);
--注：为了验证字段顺序问题，字段类型与字段是不对应的

--hive中的表
create table temp.cpa_event_params_daily_dy (
product_key    string,
product_name   string,
app_ver_code   string,
app_channel_id string,
event_name     string,
param_name     string,
param_val      string,
val_cnt        bigint,
val_pct        decimal(8,4),
src_file_day   string
);

sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table TEST2_DY \
--export-dir /user/hive/warehouse/temp.db/cpa_event_params_daily_dy \
--columns src_file_day,product_key,product_name,app_ver_code,app_channel_id,event_name,param_name,param_val,val_cnt,val_pct \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' 
结果：导出成功,但是数据是按照hive表中的字段顺序导出的，与oracle表的字段顺序不一致


3.有分区，字段一一对应
create table TEST_DY
(
product_key    VARCHAR2(8),
product_name   VARCHAR2(12),
app_ver_code   VARCHAR2(12),
app_channel_id VARCHAR2(20),
event_name     VARCHAR2(32),
param_name     VARCHAR2(32),
param_val      VARCHAR2(64),
val_cnt        NUMBER(32),
val_pct        NUMBER(8,4),
src_file_day   CHAR(8)
);

--hive中的表
create table app.cpa_event_params_daily (
product_key    string,
product_name   string,
app_ver_code   string,
app_channel_id string,
event_name     string,
param_name     string,
param_val      string,
val_cnt        bigint,
val_pct        decimal(8,4)
)
partitioned by (src_file_day string);

sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table TEST_DY \
--export-dir /user/hive/warehouse/app.db/cpa_event_params_daily/src_file_day=20170315 \
--columns product_key,product_name,app_ver_code,app_channel_id,event_name,param_name,param_val,val_cnt,val_pct,src_file_day \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' 
结果：导出失败
原因：hdfs文件上的数据并不包括src_file_day这给字段的数据


sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table TEST_DY \
--export-dir /user/hive/warehouse/app.db/cpa_event_params_daily/src_file_day=20170315 \
--columns product_key,product_name,app_ver_code,app_channel_id,event_name,param_name,param_val,val_cnt,val_pct \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' 
结果：导出成功，但是没有src_file_day这个字段的数据


sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table TEST_DY \
--export-dir /user/hive/warehouse/app.db/cpa_event_params_daily/src_file_day=20170315 \
--columns val_pct,src_file_day \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' 

结果：导出成功，把hive表中前两个字段(product_key,product_name)的数据给了oracle中的val_pct,src_file_day字段
按照这样的推论：应该把hive最后一个字段的数据给oracle中的src_file_day字段


去掉--columns参数
sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table TEST_DY \
--export-dir /user/hive/warehouse/app.db/cpa_event_params_daily/src_file_day=20170315 \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' 
导出失败


4.在hive表中增加一个src_day字段
create table temp.cpa_event_params_daily2_dy (
product_key    string,
product_name   string,
app_ver_code   string,
app_channel_id string,
event_name     string,
param_name     string,
param_val      string,
val_cnt        bigint,
val_pct        decimal(8,4),
src_day   string
)
partitioned by (src_file_day string);


with stg_cpa_event_params_daily as
(
select a1.event_name
,a1.param_name
,a1.param_val
from (
select a.event_name
,a.param_name
,a.param_val
,sum(a.val_cnt)
,row_number()over(partition by a.event_name, a.param_name, a.param_val order by sum(a.val_cnt) desc) param_val_rank
from stg.cpa_event_params_daily_01 a
where a.app_channel_id = '-1' and a.product_key= '-1' and a.app_ver_code = '-1'
group by a.event_name
,a.param_name
,a.param_val
) a1
where a1.param_val_rank <= 1000
)
insert overwrite table temp.cpa_event_params_daily2_dy partition (src_file_day = 20170315)
select t1.product_key
,if(t1.product_key=-1, '-1', nvl(b1.product_name,'')) as product_name
,t1.app_ver_code
,t1.app_channel_id
,t1.event_name
,t1.param_name
,t1.param_val
,t1.val_cnt
,if(t1.all_val_cnt = 0, 0, round(t1.val_cnt/t1.all_val_cnt, 4)) as val_pct
,'20170315' as src_day
from (
select a.app_channel_id
,a.product_key
,a.app_ver_code
,a.event_name
,a.param_name
,nvl(b.param_val,'-998') as param_val
,sum(a.val_cnt) as val_cnt
,sum(sum(a.val_cnt)) over(partition by a.app_channel_id, a.product_key, a.app_ver_code, a.event_name, a.param_name) as all_val_cnt
from stg.cpa_event_params_daily_01 a
left join stg_cpa_event_params_daily b
on a.event_name = b.event_name and a.param_name = b.param_name and a.param_val = b.param_val
group by a.app_channel_id
,a.product_key
,a.app_ver_code
,a.event_name
,a.param_name
,nvl(b.param_val,'-998')
) t1
left join mscdata.dim_kesheng_sdk_product b1 on t1.product_key = b1.product_key
where b1.product_key is not null or t1.product_key = -1;


create table cdmp_dmt.cpa_event_params_daily
(
   src_file_day         char(8),
   product_key          varchar2(8),
   product_name         varchar2(12),
   app_ver_code         varchar2(12),
   app_channel_id       varchar2(20),
   event_name           varchar2(32),
   param_name           varchar2(32),
   param_val            varchar2(64),
   val_cnt              number(32),
   val_pct              number(8,4)
)
nologging
partition by range (src_file_day)
  (partition p_max values less than (maxvalue));


sqoop export --connect jdbc:oracle:thin:@172.16.14.201:1521:cdmpdb1 \
--username cdmp_dmt \
--password \!2012cdmp_dmt\! \
--table cpa_event_params_daily \
--export-dir /user/hive/warehouse/temp.db/cpa_event_params_daily2_dy/src_file_day=20170315 \
--columns product_key,product_name,app_ver_code,app_channel_id,event_name,param_name,param_val,val_cnt,val_pct,src_file_day \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' 

注：columns 对应的是oracle中表的字段名

错误：unique constraint (CDMP_DMT.UIDX_CPA_EVENT_OCCUR_D) violated
说明插入的记录中有相同记录

总结：
背景：用sqoop把hive分区表中的数据导出到oralce中
1.hdfs文件上的数据并不包括src_file_day这个字段的数据，所以需要增加一个时间字段
2.--columns参数指的是oracle表的字段名
3.导出时，数据是按hive的字段顺序导出的，所以要注意各字段应该赋给谁!!!!
