
create table app.user_visit_detail_sx0290_monthly (
phone_number   string,
visit_date     string,
busi_prod_name string
)
partitioned by (src_file_month string)
row format delimited fields terminated by '31'
stored as textfile;


insert overwrite table app.user_visit_detail_sx0290_monthly partition (src_file_month = ${SRC_FILE_MONTH})
select a.serv_number as phone_number
      ,a.src_file_day as visit_date                             
      ,nvl(c.term_video_type_name, 'other') as busi_prod_name	
  from rptdata.fact_user_visit_hourly a  
 inner join rptdata.dim_region b 
    on a.phone_city_id = b.region_id and b.prov_id = '0290'     --陕西省	
  left join rpt.dim_term_prod_v c 
    on a.term_prod_id = c.term_prod_id 
 where a.src_file_day >= '${MONTH_START_DAY}'
   and a.src_file_day <= '${MONTH_END_DAY}' 
 group by a.serv_number ,a.src_file_day ,c.term_video_type_name; 

-------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------- 
  
--数据源  
desc rptdata.fact_user_visit_hourly;
OK
user_key                bigint                                      
busi_time               string                                      
serv_number             string                                      
user_num_type           string                                      
user_id                 string                                      
imei                    string                                      
cookie                  string                                      
tourist_visit_id        string                                      
node_id                 string                                      
sup_node_id             string                                      
page_url                string                                      
sup_page_url            string                                      
src_chn_id              string                                      
chn_id                  string                                      
chn_99000               string                                      
chn_reserve1            string                                      
version_id              string                                      
cp_id                   string                                      
content_id              string                                      
node_type               string                                      
net_type_id             string                                      
device_ua               string                                      
client_ip               string                                      
session_id              string                                      
refer_page_id           string                                      
client_id               string                                      
server_app_tag          string                                      
app_uid                 string                                      
imei_src                string                                      
sales_id                string                                      
program_id              string                                      
is_valid_visit          string                  y/n                 
is_voms                 string                  y/n                 
city_id                 string                                      
phone_city_id           string                                      
term_prod_id            string  ------终端产品ID                                      
sub_busi_id             string                                      
user_type_id            string                                      
src_file_day            string                                      
src_file_hour           string


desc rptdata.dim_region;
OK
prov_id                 string                                      
prov_name               string                                      
region_id               string                                      
region_name             string                                      
dw_create_by            string                                      
dw_create_time          string                                      
dw_update_by            string                                      
dw_update_time          string                                      
dw_delete_flag          string                  Y/N                 
dw_crc                  string                                      



--测试
select a.serv_number as phone_number
      ,a.src_file_day as visit_date
      ,c.term_video_type_name as busi_prod_name     --存在null值
  from rptdata.fact_user_visit_hourly a
  left join rpt.dim_term_prod_v c               --(left --> inner ??)
    on a.term_prod_id = c.term_prod_id 
 inner join rptdata.dim_region d 
    on a.phone_city_id = d.region_id and d.prov_id = '0290'
 where a.src_file_day >= '20170301'
   and a.src_file_day <= '20170331'
 group by a.serv_number ,a.src_file_day ,c.term_video_type_name
 limit 1000;
 
15029926000	20170317	咪咕视频	201703
14729088631	20170317	NULL	201703
13991262799	20170317	NULL	201703
15291439687	20170317	咪咕影院	201703
13619242472	20170317	咪咕视频	201703
18829451299	20170317	咪咕视频	201703
15829313491	20170317	咪咕影院	201703
15029592724	20170317	咪咕视频	201703
15706001002	20170317	咪咕影院	201703
13720682539	20170317	咪咕视频	201703
13909120009	20170317	咪咕影院	201703
13992574666	20170317	咪咕影院	201703
15009163536	20170317	咪咕影院	201703
13892163374	20170317	咪咕影院	201703
13700205167	20170317	咪咕影院	201703
18329504474	20170317	咪咕视频	201703
18791515845	20170317	咪咕影院	201703
18209234592	20170317	咪咕影院	201703
15929199752	20170317	咪咕视频	201703
15291423352	20170317	咪咕视频	201703
13572640461	20170317	和视频	201703
15706005098	20170317	NULL	201703
13629258928	20170317	和视频	201703
18829575745	20170317	咪咕影院	201703
15877536659	20170317	咪咕视频	201703
13572870737	20170317	咪咕影院	201703
15129777803	20170317	咪咕视频	201703
15291727350	20170317	咪咕影院	201703
18700082627	20170317	咪咕影院	201703
13709251160	20170317	和视频	201703
13474387621	20170317	和视频	201703
18821620837	20170317	咪咕影院	201703
15229563829	20170317	咪咕视频	201703
13892288269	20170317	咪咕视频	201703
13992978630	20170317	NULL	201703
14729265013	20170317	咪咕视频	201703
15029445696	20170317	NULL	201703
18792908633	20170317	咪咕影院	201703
18292930447	20170317	咪咕影院	201703
13772280281	20170317	和视频	201703
13720743058	20170317	咪咕视频	201703
13992281270	20170317	咪咕影院	201703
13474314824	20170317	咪咕视频	201703
15706005098	20170317	咪咕影院	201703
15109262069	20170317	咪咕视频	201703
13649280401	20170317	咪咕视频	201703
15029454034	20170317	咪咕影院	201703
13659109428	20170317	NULL	201703
15191409307	20170317	咪咕视频	201703
18829293355	20170317	咪咕影院	201703
18291121798	20170317	咪咕视频	201703
13892039647	20170317	咪咕影院	201703
15129405204	20170317	咪咕影院	201703
18391457410	20170317	咪咕影院	201703
18291098766	20170317	咪咕影院	201703
13474543093	20170317	咪咕影院	201703
13892612156	20170317	咪咕影院	201703
13468503463	20170317	咪咕视频	201703
18391506122	20170317	咪咕视频	201703