1.稽核3、4没有看懂，感觉没有match文档上对应的稽核规则,希望讲解下逻辑

这两个稽核是用在哪里的？？代码体现在哪     前3个IP

2.使用收入类的表 intdata.ugc_90104_monthorder_union 口径是有变化的，并没有找到 order_status为'4'的条目 
3.稽核5 是用在哪里的？？代码体现在哪

--4.cdmpview.tmp_wsj_05_Assess_2v3 没有相应的表

5.前后两次提供的Assess_4和Assess_5中的副表cdmp_dw.tdim_biz_name_v,cdmpview.tmp_wsj_Assess_4_program不同的,有什么区别吗？
节目与业务的id信息命名反了，具体是什么意思？


取数平台
http://117.131.17.93:8042/portal/auth/login
wangshujie/111111




    > desc rptdata.fact_use_detail;
OK
play_session_id     	string             	                    
usernum_id          	string           ----   	                    
user_id             	string              	                    
user_type_id        	string           ----   	                    
broadcast_type_id   	string              	                    
use_begin_time      	string              	                    
use_end_time        	string              	                    
duration_sec        	bigint          ----    	                    
flow_kb             	bigint          ----    	                    
use_source_ip       	string          ----    	                    
program_id          	string              	                    
chrgprod_id         	string              	                    
content_id          	string              	                    
net_type_id         	string              	                    
plat_id             	string              	                    
channel_id          	string              	                    
app_version_code    	string              	                    
visit_session_id    	string              	                    
imei                	string              	                    
region_id           	string              	                    
ip_prov_id          	string              	                    
termprod_id         	string              	                    
sub_busi_id         	string              	                    
play_url_domain     	string              	                    
program_type_id     	string              	                    
isp_id              	string              	                    
vv                  	bigint              	                    
cdn_id              	string              	                    
page_id             	string              	                    
position_id         	string              	                    
msisdn_region_id    	string              	                    
user_num_type       	string              	                    
source_channel_value	string              	                    
domain_belong_type_id	string              	                    
preview_flag        	string              	preview play flage  
advertise_flag      	string              	advertisement play flage
src_file_day        	string              	                    
service_url_domain  	string 


    > desc cdmp_dw.tdim_biz_name_v;
OK
b_type_id           	string              	                    
b_type              	string              	                    
business_id         	string        -----     	                    
business_name       	string              	                    
sub_busi_id         	string        ----      	                    
sub_busi_name       	string              	                    
src_file_day        	string 

业务维表 大数据:
rptdata.dim_business  
（SELECT /*+parallel(t,32)*/DISTINCT a.b_type_id
                   ,a.b_type
				   ,a.business_id
				   ,a.business_name
			   FROM cdmp_dw.tdim_biz_name_v a）
	/CDMP:cdmp_dw.tdim_biz_name_v

子业务维表 大数据:
rptdata.dim_sub_busi   
（SELECT /*+parallel(t,32)*/*
	FROM cdmp_dw.tdim_biz_name_v a
   WHERE a.sub_busi_id IS NOT NULL）
	/CDMP:cdmp_dw.tdim_biz_name_v

    > desc rptdata.fact_user_visit_hourly;
OK
user_key            	bigint             ----- 	                    
busi_time           	string              	                    
serv_number         	string              	                    
user_num_type       	string              	                    
user_id             	string              	                    
imei                	string              	                    
cookie              	string              	                    
tourist_visit_id    	string              	                    
node_id             	string              	                    
sup_node_id         	string              	                    
page_url            	string              	                    
sup_page_url        	string              	                    
src_chn_id          	string              	                    
chn_id              	string              	                    
chn_99000           	string              	                    
chn_reserve1        	string              	                    
version_id          	string              	                    
cp_id               	string              	                    
content_id          	string              	                    
node_type           	string              	                    
net_type_id         	string              	                    
device_ua           	string              	                    
client_ip           	string       -----       	                    
session_id          	string              	                    
refer_page_id       	string              	                    
client_id           	string              	                    
server_app_tag      	string              	                    
app_uid             	string              	                    
imei_src            	string              	                    
sales_id            	string              	                      
program_id          	string              	                    
is_valid_visit      	string              	y/n                 
is_voms             	string              	y/n                 
city_id             	string              	                    
phone_city_id       	string              	                    
term_prod_id        	string              	                    
sub_busi_id         	string              	                    
user_type_id        	string          ------    	                    
src_file_day        	string              	                    
src_file_hour       	string



hive> desc cdmpview.tmp_wsj_0606_dim_busi_new;
OK
dept                	string              	                    
b_type              	string              	                    
business_id         	string              	                    
dept_id             	string


hive> desc rptdata.dim_dept_term_prod;
OK
dept_id             	string              	                    
dept_name           	string              	                    
term_prod_id        	string              	                    
term_prod_name      	string              	                    
term_video_type_name	string              	                    
portal_type         	string              	                    
done_date           	string              	                    
expire_date         	string              	                    
flag                	string              	                    
dw_create_by        	string              	                    
dw_create_time      	string              	                    
dw_update_by        	string              	                    
dw_update_time      	string              	                    
dw_delete_flag      	string              	Y/N                 
dw_crc              	string 



hive> desc rptdata.dim_dept_chn;
OK
dept_id             	string              	                    
dept_name           	string              	                    
chn_id              	string              	                    
chn_class           	string              	                    
chn_type            	string              	                    
chn_type_2          	string              	                    
chn_type_2_new      	string              	                    
chn_attr_1_name     	string              	                    
chn_attr_2_name     	string              	                    
chn_attr_3_name     	string              	                    
chn_attr_4_name     	string              	                    
prod_name           	string              	                    
has_contract        	string              	                    
dw_create_by        	string              	                    
dw_create_time      	string              	                    
dw_update_by        	string              	                    
dw_update_time      	string              	                    
dw_delete_flag      	string              	Y/N                 
dw_crc              	string  

    > desc cdmpview.tmp_wsj_Assess_4_program;
OK
program_id          	string              	                    
business_id         	string 