--原始版本=========================================================
drop table if exists cdmp.tmp_wsj_0217_fj_u ;

create table cdmp.tmp_wsj_0217_fj_u as
select b.usernum_id, sum(b.duration_sec)duration_sec
from(
select *
from rptdata.fact_use_detail a 
where a.src_file_day between'20170121'and'20170216'
and a.termprod_id in (select term_prod_id from rptdata.dim_prod_id where term_video_type_name='咪咕视频'))b
where b.usernum_id in (select usernum  from dly.fujian20170217)
group by b.usernum_id;

--我修改的版本=====================================================
select 
	t1.usernum_id
	,sum(t1.duration_sec)
from  
	(
	select a.usernum_id
	,a.duration_sec
	,b.term_video_type_name
	from rptdata.fact_use_detail a 
	left join rptdata.dim_prod_id b 
		   on a.termprod_id = b.term_prod_id
	where a.src_file_day between'20170121'and'20170216'
	) t1 
inner join dly.fujian20170217 t2 on t1.usernum_id = t2.usernum 
where t1.term_video_type_name = '咪咕视频' 
group by t1.usernum_id;
 
 
-- 数据源======================================
desc rptdata.fact_use_detail;		a
OK
play_session_id         string                                      
usernum_id              string     -------------                                  
user_id                 string                                     
user_type_id            string                                      
broadcast_type_id       string                                      
use_begin_time          string                                      
use_end_time            string                                      
duration_sec            bigint                                      
flow_kb                 bigint                                      
use_source_ip           string                                      
program_id              string                                      
chrgprod_id             string                                      
content_id              string                                      
net_type_id             string                                      
plat_id                 string                                      
channel_id              string                                      
app_version_code        string                                      
visit_session_id        string                                      
imei                    string                                      
region_id               string                                      
ip_prov_id              string                                      
termprod_id             string      ----------                              
sub_busi_id             string                                      
play_url_domain         string                                      
program_type_id         string                                      
isp_id                  string                                      
vv                      bigint                                      
cdn_id                  string                                      
page_id                 string                                      
position_id             string                                      
msisdn_region_id        string                                      
user_num_type           string                                      
source_channel_value    string                                      
domain_belong_type_id   string                                      
src_file_day            string                                      
service_url_domain      string                                      
                 
# Partition Information          
# col_name              data_type               comment             
                 
src_file_day            string                                      
service_url_domain      string 


desc rptdata.dim_prod_id;			b
OK
term_prod_id            string       -----------                       
term_prod_name          string                                      
term_prod_type_id       string                                      
term_prod_type_name     string                                      
term_prod_class_id      string                                      
term_prod_class_name    string                                      
term_version_id         string                                      
term_video_type_id      string                                      
term_video_type_name    string                                      
term_video_soft_id      string                                      
term_video_soft_name    string                                      
dw_create_by            string                                      
dw_create_time          string                                      
dw_update_by            string                                      
dw_update_time          string                                      
dw_delete_flag          string                                      
dw_crc                  string  


 desc dly.fujian20170217;			c
OK
usernum                 string   




-- 测试的另一版本===========================================================
select 
	t1.usernum
	,sum(t2.duration_sec)
from dly.fujian20170217 t1
left join
	(
	select a.usernum_id
		  ,a.duration_sec
		  ,b.term_video_type_name
	 from rptdata.fact_use_detail a 
	 left join rptdata.dim_prod_id b on a.termprod_id = b.term_prod_id
	where a.src_file_day between'20170121'and'20170216'
	) t2 
on t1.usernum = t2.usernum_id 
where t2.term_video_type_name = '咪咕视频' 
group by t1.usernum;