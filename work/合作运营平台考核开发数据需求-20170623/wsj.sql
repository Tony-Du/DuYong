--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_check_1 ;

CREATE TABLE cdmpview.tmp_wsj_05_check_1 AS
SELECT '201705' statis_month,  --统计月
       c.business_id,         --业务ID
       a.usernum_id,          --用户号码
       SUM(a.flow_kb) flow_kb --当月 该业务下 人均 使用流量kb
  FROM rptdata.fact_use_detail a
  join (
        select business_id
		      ,sub_busi_id 
          from cdmp_dw.tdim_biz_name_v 
         where src_file_day='20170612'
         group by business_id,sub_busi_id
       ) c
    on a.sub_busi_id = c.sub_busi_id
 WHERE a.src_file_day between '20170501' and '20170531'
   and a.user_type_id = '1'  --注册用户，0表示游客
   and c.business_id in (
'B1000100000',
'B1000100020',
'B1000100034',
'B1000101400',
'B1000101401',
'B1000101500',
'B2000100001',
'B2000100021',
'B2000100022',
'B2000100031',
'B2000100036',
'B2000100037',
'B2000100038',
'B2000100039',
'B2000100040',
'B2000100041',
'B2000100042',
'B2000100045',
'B2000101701',
'B2000101702',
'B2000101703',
'B2000101704',
'B2000101705',
'B2000101800',
'B2000101801',
'B2000101802',
'B2000101803',
'B2000101804',
'B2000102101',
'B2000102102',
'B2000102301',
'B2000102401',
'B2000102402',
'B2000102801',
'B2000102901',
'B2000102902',
'B2000103301',
'B2000103701',
'B2000103702',
'B2000103801',
'B2000104001',
'B2000104402',
'BA000104501',
'BA000104502',
'BA000104503',
'BA000104504',
'BA000104505',
'BA000104506',
'BA000104601',
'BA000104602',
'BA000104603',
'BC000103501',
'BC000103503',
'BC000103504',
'BC000103505',
'BC000103506',
'BC000103507',
'BC000103508',
'BC000103509',
'BC000103510',
'BC000103601',
'BC000104604'
)
GROUP BY c.business_id, a.usernum_id;

============== check_2 =========================

--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_check_2 ;
CREATE TABLE cdmpview.tmp_wsj_05_check_2 AS
SELECT '201705' statis_month,
       c.business_id,
       a.usernum_id,
       SUM(a.duration_sec) duration_sec --当月 该业务下 人均 使用时长s
FROM rptdata.fact_use_detail a
join (select business_id,sub_busi_id 
        from cdmp_dw.tdim_biz_name_v 
       where src_file_day='20170612'
       group by business_id,sub_busi_id
	 ) c
on a.sub_busi_id = c.sub_busi_id
WHERE a.src_file_day between '20170501'and'20170531'
and a.user_type_id='1'
and c.business_id in (
'B1000100000',
'B1000100020',
'B1000100034',
'B1000101400',
'B1000101401',
'B1000101500',
'B2000100001',
'B2000100021',
'B2000100022',
'B2000100031',
'B2000100036',
'B2000100037',
'B2000100038',
'B2000100039',
'B2000100040',
'B2000100041',
'B2000100042',
'B2000100045',
'B2000101701',
'B2000101702',
'B2000101703',
'B2000101704',
'B2000101705',
'B2000101800',
'B2000101801',
'B2000101802',
'B2000101803',
'B2000101804',
'B2000102101',
'B2000102102',
'B2000102301',
'B2000102401',
'B2000102402',
'B2000102801',
'B2000102901',
'B2000102902',
'B2000103301',
'B2000103701',
'B2000103702',
'B2000103801',
'B2000104001',
'B2000104402',
'BA000104501',
'BA000104502',
'BA000104503',
'BA000104504',
'BA000104505',
'BA000104506',
'BA000104601',
'BA000104602',
'BA000104603',
'BC000103501',
'BC000103503',
'BC000103504',
'BC000103505',
'BC000103506',
'BC000103507',
'BC000103508',
'BC000103509',
'BC000103510',
'BC000103601',
'BC000104604'
)
GROUP BY c.business_id, a.usernum_id;

============= check_3 =====================
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_check_3_u_detail ;
CREATE TABLE cdmpview.tmp_wsj_05_check_3_u_detail AS
select a.src_file_day,  
       a.use_source_ip, --用户源IP
       a.usernum_id,    --使用用户
       c.business_id       
FROM rptdata.fact_use_detail a   
join (
      select business_id,sub_busi_id 
        from cdmp_dw.tdim_biz_name_v 
       where src_file_day='20170612'
       group by business_id,sub_busi_id
	 ) c
on a.sub_busi_id=c.sub_busi_id
WHERE a.src_file_day between '20170501' and '20170531'
  and a.user_type_id = '1'
  and c.business_id in (
'B1000100000',
'B1000100020',
'B1000100034',
'B1000101400',
'B1000101401',
'B1000101500',
'B2000100001',
'B2000100021',
'B2000100022',
'B2000100031',
'B2000100036',
'B2000100037',
'B2000100038',
'B2000100039',
'B2000100040',
'B2000100041',
'B2000100042',
'B2000100045',
'B2000101701',
'B2000101702',
'B2000101703',
'B2000101704',
'B2000101705',
'B2000101800',
'B2000101801',
'B2000101802',
'B2000101803',
'B2000101804',
'B2000102101',
'B2000102102',
'B2000102301',
'B2000102401',
'B2000102402',
'B2000102801',
'B2000102901',
'B2000102902',
'B2000103301',
'B2000103701',
'B2000103702',
'B2000103801',
'B2000104001',
'B2000104402',
'BA000104501',
'BA000104502',
'BA000104503',
'BA000104504',
'BA000104505',
'BA000104506',
'BA000104601',
'BA000104602',
'BA000104603',
'BC000103501',
'BC000103503',
'BC000103504',
'BC000103505',
'BC000103506',
'BC000103507',
'BC000103508',
'BC000103509',
'BC000103510',
'BC000103601',
'BC000104604'
)
group by a.src_file_day,a.use_source_ip,a.usernum_id,c.business_id;
		
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_check_3_u ;
CREATE TABLE cdmpview.tmp_wsj_05_check_3_u AS
SELECT '201705'statis_month,
       v.business_id,
       v.use_source_ip,                -- IP
       SUM(v.check_3_u_num) check_3_u_num, --当月 每个IP下 该业务的使用用户数
       ROW_NUMBER() OVER(PARTITION BY v.business_id Order by SUM(v.check_3_u_num) DESC) nn
	   --按照 业务ID 分组，对 使用用户数 进行排序
from(
     select src_file_day
	       ,use_source_ip
		   ,business_id
		   ,count(usernum_id) check_3_u_num --每天、每个IP下 该业务的使用用户数
       from cdmpview.tmp_wsj_05_check_3_u_detail
      group by src_file_day, use_source_ip, business_id
	) v
group by v.business_id,
         v.use_source_ip;



--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_check_3 ;
CREATE TABLE cdmpview.tmp_wsj_05_check_3 AS
SELECT a.business_id,
       sum(CASE WHEN a.nn<=3 THEN a.check_3_u_num ELSE 0 END) AS in_3_check_3_num,
	   --业务使用用户数为前3名的 当月 该业务的使用用户数
       SUM(a.check_3_u_num) all_check_3_num
  FROM cdmpview.tmp_wsj_05_check_3_u a
 GROUP BY a.business_id;


=========== check_4 =================

--DROP table cdmpview.tmp_wsj_05_check_4_userkey_detail purge;
CREATE TABLE cdmpview.tmp_wsj_05_check_4_userkey_detail AS
SELECT a.src_file_day,
       c.business_id,
       a.client_ip,   --客户端IP
       a.user_key     --访问用户号码
FROM rptdata.fact_user_visit_hourly a  --用户访问表
join (
      select business_id,
	         sub_busi_id 
        from cdmp_dw.tdim_biz_name_v 
       where src_file_day='20170612'
       group by business_id,sub_busi_id
     ) c
  on a.sub_busi_id = c.sub_busi_id
WHERE a.src_file_day between '20170501'and'20170531'
and c.business_id in (
'B1000100000',
'B1000100020',
'B1000100034',
'B1000101400',
'B1000101401',
'B1000101500',
'B2000100001',
'B2000100021',
'B2000100022',
'B2000100031',
'B2000100036',
'B2000100037',
'B2000100038',
'B2000100039',
'B2000100040',
'B2000100041',
'B2000100042',
'B2000100045',
'B2000101701',
'B2000101702',
'B2000101703',
'B2000101704',
'B2000101705',
'B2000101800',
'B2000101801',
'B2000101802',
'B2000101803',
'B2000101804',
'B2000102101',
'B2000102102',
'B2000102301',
'B2000102401',
'B2000102402',
'B2000102801',
'B2000102901',
'B2000102902',
'B2000103301',
'B2000103701',
'B2000103702',
'B2000103801',
'B2000104001',
'B2000104402',
'BA000104501',
'BA000104502',
'BA000104503',
'BA000104504',
'BA000104505',
'BA000104506',
'BA000104601',
'BA000104602',
'BA000104603',
'BC000103501',
'BC000103503',
'BC000103504',
'BC000103505',
'BC000103506',
'BC000103507',
'BC000103508',
'BC000103509',
'BC000103510',
'BC000103601',
'BC000104604'
) 
GROUP BY a.src_file_day,
         c.business_id,
         a.client_ip,
         a.user_key;


--DROP table cdmpview.tmp_wsj_05_check_4_userkey_all purge;
CREATE TABLE cdmpview.tmp_wsj_05_check_4_userkey_all AS
SELECT '201705'statis_month,
       a.business_id,
       a.client_ip,
       SUM(a.check_4_num) check_4_num,
       ROW_NUMBER() OVER(PARTITION BY a.business_id Order by SUM(a.check_4_num) DESC) nn
	   -- 按照 业务ID 进行分组，对 用户key 进行排序	   
FROM (SELECT src_file_day
            ,business_id
			,client_ip
            ,count(user_key) check_4_num  
        FROM cdmpview.tmp_wsj_05_check_4_userkey_detail
       group by src_file_day,business_id, client_ip
	 )a
GROUP BY a.business_id,
         a. client_ip;

  
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_check_4_userkey ;
CREATE TABLE cdmpview.tmp_wsj_05_check_4_userkey AS
SELECT a.business_id,
       sum(CASE WHEN a.nn<=3 THEN a.check_4_num ELSE 0 END) AS in_3_check_4_num,
	   --取前3名
       SUM (a.check_4_num) all_check_4_num
FROM cdmpview.tmp_wsj_05_check_4_userkey_all a
GROUP BY a.business_id;
--SELECT * FROM cdmpview.tmp_wsj_05_check_4_userkey;   


=================== Assess_2 ============================================
--使用用户数与在订付费用户比例:
--分子为考核月使用合作伙伴业务的 注册用户数，分母为考核月月末合作伙伴业务 付费在订用户数

--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_2_u_de ;    
CREATE TABLE cdmpview.tmp_wsj_05_Assess_2_u_de AS

SELECT '201705' statis_month,
       CASE WHEN b.term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(d.dept_name,'9999') 
	        ELSE nvl(b.dept_name, nvl(c.dept, nvl(d.dept_name, '9999'))) END dept, --部门
       cc.business_id,
	   a.usernum_id      --使用合作伙伴业务的 注册用户
 FROM rptdata.fact_use_detail a  --使用明细表
join (select business_id,sub_busi_id 
        from cdmp_dw.tdim_biz_name_v 
       where src_file_day='20170612'
       group by business_id,sub_busi_id
	 ) cc
  on a.sub_busi_id=cc.sub_busi_id
  LEFT JOIN rptdata.dim_dept_term_prod b 
  ON a.termprod_id=b.term_prod_id
  LEFT JOIN cdmpview.tmp_wsj_0606_dim_busi_new c 
  ON cc.business_id=c.business_id
  LEFT JOIN  rptdata.dim_dept_chn d 
  ON a.channel_id=d.chn_id
  WHERE a.src_file_day between '20170501'and'20170531'
  and a.user_type_id='1'
and cc.business_id in (
                       'B1000100000',
                       'B1000100020',
                       'B1000100034',
                       'B1000101400',
                       'B1000101401',
                       'B1000101500',
                       'B2000100001',
                       'B2000100021',
                       'B2000100022',
                       'B2000100031',
                       'B2000100036',
                       'B2000100037',
                       'B2000100038',
                       'B2000100039',
                       'B2000100040',
                       'B2000100041',
                       'B2000100042',
                       'B2000100045',
                       'B2000101701',
                       'B2000101702',
                       'B2000101703',
                       'B2000101704',
                       'B2000101705',
                       'B2000101800',
                       'B2000101801',
                       'B2000101802',
                       'B2000101803',
                       'B2000101804',
                       'B2000102101',
                       'B2000102102',
                       'B2000102301',
                       'B2000102401',
                       'B2000102402',
                       'B2000102801',
                       'B2000102901',
                       'B2000102902',
                       'B2000103301',
                       'B2000103701',
                       'B2000103702',
                       'B2000103801',
                       'B2000104001',
                       'B2000104402',
                       'BA000104501',
                       'BA000104502',
                       'BA000104503',
                       'BA000104504',
                       'BA000104505',
                       'BA000104506',
                       'BA000104601',
                       'BA000104602',
                       'BA000104603',
                       'BC000103501',
                       'BC000103503',
                       'BC000103504',
                       'BC000103505',
                       'BC000103506',
                       'BC000103507',
                       'BC000103508',
                       'BC000103509',
                       'BC000103510',
                       'BC000103601',
                       'BC000104604'
                      )
group by CASE WHEN term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(d.dept_name,'9999') 
              ELSE nvl(b.dept_name,nvl(c.dept,nvl(d.dept_name,'9999'))) END,
         cc.business_id,
		 a.usernum_id;
           
           

--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_2_u ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_2_u AS
select v.statis_month
      ,v.business_id
	  ,count(v.usernum_id) user_unum  --稽核后的 使用合作伙伴业务的 注册用户数 (即不含游客)
from(
  SELECT '201705'statis_month
        ,a.business_id
		,a.usernum_id
    FROM cdmpview.tmp_wsj_05_Assess_2_u_de a
    join cdmpview.tmp_wsj_05_check_1 b  --流量kb
      on a.usernum_id=b.usernum_id and a.business_id=b.business_id
    join cdmpview.tmp_wsj_05_check_2 c  --播放时长s
      on a.usernum_id=c.usernum_id and a.business_id=c.business_id
   WHERE a.statis_month='201705' 
     AND a.dept='合作运营部'
     and b.flow_kb >= 0.3*1024
     and c.duration_sec >= 1
   GROUP BY a.business_id,a.usernum_id
	) v
group by v.statis_month,v.business_id;


--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_2_order_detail ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_2_order_detail AS
SELECT '201705' statis_month,
       cc.business_id,
	   a.usernum           --合作伙伴业务付费 在订用户号码
  FROM intdata.ugc_90104_monthorder_union a  
  join rptdata.dim_charge_product c
    on a.product_id=c.chrgprod_id 
  join (
        select business_id,
               sub_busi_id 
          from cdmp_dw.tdim_biz_name_v 
         where src_file_day='20170612'
         group by business_id,sub_busi_id
  	   ) cc 
    on c.sub_busi_bdid = cc.sub_busi_id
 WHERE a.src_file_day='20170531'
   AND a.order_status <> '4' --表示什么。收入类口径有变化
   AND c.chrgprod_price > 0 
   and cc.business_id in (
'B1000100000',
'B1000100020',
'B1000100034',
'B1000101400',
'B1000101401',
'B1000101500',
'B2000100001',
'B2000100021',
'B2000100022',
'B2000100031',
'B2000100036',
'B2000100037',
'B2000100038',
'B2000100039',
'B2000100040',
'B2000100041',
'B2000100042',
'B2000100045',
'B2000101701',
'B2000101702',
'B2000101703',
'B2000101704',
'B2000101705',
'B2000101800',
'B2000101801',
'B2000101802',
'B2000101803',
'B2000101804',
'B2000102101',
'B2000102102',
'B2000102301',
'B2000102401',
'B2000102402',
'B2000102801',
'B2000102901',
'B2000102902',
'B2000103301',
'B2000103701',
'B2000103702',
'B2000103801',
'B2000104001',
'B2000104402',
'BA000104501',
'BA000104502',
'BA000104503',
'BA000104504',
'BA000104505',
'BA000104506',
'BA000104601',
'BA000104602',
'BA000104603',
'BC000103501',
'BC000103503',
'BC000103504',
'BC000103505',
'BC000103506',
'BC000103507',
'BC000103508',
'BC000103509',
'BC000103510',
'BC000103601',
'BC000104604'
)
GROUP BY cc.business_id,
         a.usernum;

--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_2_order ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_2_order AS
select a.statis_month,
       a.business_id,
	   count(a.usernum) user_unum --合作伙伴业务付费在订用户数
from cdmpview.tmp_wsj_05_Assess_2_order_detail a
group by a.statis_month,a.business_id;


--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_2 ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_2 AS
SELECT nvl(u.statis_month, ord.statis_month ) statis_month,
       nvl(u.business_id, ord.business_id) business_id,
       nvl(u.user_unum, 0) u_user_unum,
       nvl(ord.user_unum, 0) ord_user_unum,
       nvl(u.user_unum, 0)/nvl(ord.user_unum, 0) u_dvd_ord  --使用用户数与在订付费用户比例
FROM cdmpview.tmp_wsj_05_Assess_2_u u
FULL JOIN cdmpview.tmp_wsj_05_Assess_2_Order ord
  ON u.statis_month=ord.statis_month and u.business_id=ord.business_id;

============ Assess_3 ===========================

--考核_访问活跃度 userkey 日均访问用户数/月剔重访问用户数
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_3_U_de_userkey ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_3_U_de_userkey AS
SELECT 
     CASE WHEN term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(d.dept_name,'9999') 
	      ELSE nvl(b.dept_name,nvl(c.dept,nvl(d.dept_name,'9999'))) END dept,
     a.src_file_day,
	 cc.business_id,
     a.user_key,
	 a.user_type_id
FROM rptdata.fact_user_visit_hourly a 
join (select business_id,
             sub_busi_id 
        from cdmp_dw.tdim_biz_name_v 
       where src_file_day='20170612'
       group by business_id,sub_busi_id
	 ) cc
on a.sub_busi_id=cc.sub_busi_id
LEFT JOIN rptdata.dim_dept_term_prod  b 
ON a.term_prod_id=b.term_prod_id
LEFT JOIN cdmpview.tmp_wsj_0606_dim_busi_new  c 
ON cc.business_id=c.business_id
LEFT JOIN rptdata.dim_dept_chn   d 
ON a.chn_id=d.chn_id
WHERE a.src_file_day BETWEEN '20170501'AND'20170531'
and cc.business_id in (
'B1000100000',
'B1000100020',
'B1000100034',
'B1000101400',
'B1000101401',
'B1000101500',
'B2000100001',
'B2000100021',
'B2000100022',
'B2000100031',
'B2000100036',
'B2000100037',
'B2000100038',
'B2000100039',
'B2000100040',
'B2000100041',
'B2000100042',
'B2000100045',
'B2000101701',
'B2000101702',
'B2000101703',
'B2000101704',
'B2000101705',
'B2000101800',
'B2000101801',
'B2000101802',
'B2000101803',
'B2000101804',
'B2000102101',
'B2000102102',
'B2000102301',
'B2000102401',
'B2000102402',
'B2000102801',
'B2000102901',
'B2000102902',              
'B2000103301',              
'B2000103701',              
'B2000103702',              
'B2000103801',              
'B2000104001',              
'B2000104402',              
'BA000104501',              
'BA000104502',              
'BA000104503',              
'BA000104504',              
'BA000104505',              
'BA000104506',              
'BA000104601',
'BA000104602',
'BA000104603',
'BC000103501',
'BC000103503',
'BC000103504',
'BC000103505',
'BC000103506',
'BC000103507',
'BC000103508',
'BC000103509',
'BC000103510',
'BC000103601',
'BC000104604'
)
group by CASE WHEN term_prod_name IN ('和视频OPENAPI','和视频SDK') THEN nvl(d.dept_name,'9999') 
              ELSE nvl(b.dept_name,nvl(c.dept,nvl(d.dept_name,'9999'))) END,
         a.src_file_day,
		 cc.business_id,
         a.user_key,
		 a.user_type_id;

		 
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_3_userkey ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_3_userkey AS
SELECT a.src_file_day
      ,a.business_id
	  ,count(DISTINCT a.user_key) user_unum  --日访问用户数 剔重 （不是日均）
 FROM cdmpview.tmp_wsj_05_Assess_3_U_de_userkey a
WHERE a.dept='合作运营部'
  AND a.src_file_day BETWEEN '20170501'AND'20170531'
GROUP BY a.src_file_day,a.business_id;


--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_3_userkey_d ;   
drop table cdmpview.tmp_wsj_05_Assess_3_userkey_d purge;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_3_userkey_d AS
select a.business_id,
       sum(a.user_unum)/31 user_unum      --日均访问用户数
  from cdmpview.tmp_wsj_05_Assess_3_userkey a
 WHERE a.src_file_day BETWEEN '20170501'AND'20170531'
 group by a.business_id;


--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_3_userkey_m ;    
CREATE TABLE cdmpview.tmp_wsj_05_Assess_3_userkey_m AS
SELECT a.business_id,
       count(DISTINCT a.user_key) user_unum  --月剔重 注册 访问用户数
  FROM cdmpview.tmp_wsj_05_Assess_3_U_de_userkey a
 WHERE a.dept='合作运营部'
   and a.user_type_id='1'    --注册用户
   AND a.src_file_day between '20170501'and'20170531'
 GROUP BY a.business_id;

--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_3_userkey_mall ;    
CREATE TABLE cdmpview.tmp_wsj_05_Assess_3_userkey_mall AS
SELECT a.business_id,
       count(DISTINCT a.user_key) user_unum   --月剔重访问用户数
  FROM cdmpview.tmp_wsj_05_Assess_3_U_de_userkey a
 WHERE a.dept='合作运营部'
   AND a.src_file_day between '20170501'and'20170531'
 GROUP BY a.business_id;
 
 
SELECT a.statis_month,
       nvl(a.business_id,c.business_id) business_id,
       a.u_user_unum,
       CASE WHEN d.all_check_3_num=0 THEN 0 
	        ELSE d.in_3_check_3_num/d.all_check_3_num  --前3名占比
			END propo_3_ip_use, 
       a.ord_user_unum,
       a.u_dvd_ord, 
       f.user_unum,
       cc.user_unum user_unum_m,
       CASE WHEN e.all_check_4_num =0 THEN 0
            ELSE e.in_3_check_4_num /e.all_check_4_num  --前3名占比
            END  propo_3_ip_visit, 
       CASE WHEN cc.user_unum=0 THEN 0
            ELSE c.user_unum/cc.user_unum --访问活跃度：日均访问用户数/月剔重访问用户数
            END activ_visit,
       case when cc.user_unum=0 THEN 0
            ELSE 1-(f.user_unum/cc.user_unum)
            end tourist_propo             -- 游客占比
  FROM cdmpview.tmp_wsj_05_Assess_2 a--56
  
  FULL JOIN cdmpview.tmp_wsj_05_Assess_3_userkey_d c--61
    ON  a.business_id = c.business_id
  FULL JOIN cdmpview.tmp_wsj_05_Assess_3_userkey_mall cc--61
    ON nvl(a.business_id,c.business_id) = cc.business_id
	
  LEFT JOIN cdmpview.tmp_wsj_05_check_3 d--60
    ON nvl(a.business_id,c.business_id) = d.business_id
  LEFT JOIN cdmpview.tmp_wsj_05_check_4_userkey e--61
    ON nvl(a.business_id,c.business_id) = e.business_id
	
  left join cdmpview.tmp_wsj_05_Assess_3_userkey_m f--56
    on nvl(a.business_id,c.business_id) = f.business_id;

============= Assess_4 ===============================
--节目观看次数均值	节目人均使用次数/当月上传节目数量
--cdmpview.tmp_wsj_Assess_4_program;
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_4_detail ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_4_detail AS
select c.business_id,
       count(distinct a.usernum_id) assess_4_num, --使用人数
       count(1) assess_4_cnt,  --使用次数
       count(distinct bb.program_id) assess_4_pr_num --上传节目数量
  FROM rptdata.fact_use_detail a
join (
      select business_id,
             sub_busi_id 
        from cdmp_dw.tdim_biz_name_v 
       where src_file_day='20170612'
       group by business_id,sub_busi_id
	  ) c
on a.sub_busi_id=c.sub_busi_id
join cdmpview.tmp_wsj_Assess_4_program bb
on a.program_id=bb.program_id
and c.business_id=bb.business_id
WHERE a.src_file_day between '20170501'and'20170531'
group by c.business_id;


--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_4 ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_4 AS
select a.business_id,
       (a.assess_4_cnt/a.assess_4_num)/a.assess_4_pr_num
  from cdmpview.tmp_wsj_05_Assess_4_detail a;

============= Assess_5 ============================================
--节目观看时长均值	节目人均使用时长/当月上传节目数量

--cdmpview.tmp_wsj_Assess_4_program;
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_5_detail ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_5_detail AS
select c.business_id,
       count(distinct a.usernum_id) assess_5_num,
       sum(duration_sec) assess_5_duration_sec,
       count(distinct bb.program_id) assess_5_pr_num
  FROM rptdata.fact_use_detail a
  join (
        select business_id,sub_busi_id 
          from cdmp_dw.tdim_biz_name_v 
         where src_file_day='20170612'
         group by business_id,sub_busi_id
	   ) c
   on a.sub_busi_id=c.sub_busi_id
 join cdmpview.tmp_wsj_Assess_4_program bb
   on a.program_id=bb.program_id
  and c.business_id=bb.business_id
WHERE a.src_file_day between '20170501'and'20170531'
group by c.business_id;


--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_5 ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_5 AS
select a.business_id,
       (a.assess_5_duration_sec/a.assess_5_num)/a.assess_5_pr_num
  from cdmpview.tmp_wsj_05_Assess_5_detail a;




在对Assess_4、Assess_5进行处理时发现，之前给到的严格口径会使输出结果为0，和芳达确认后调整口径如下，还请知晓。
另外在数据导入时，将节目与业务的id信息命名反了，还请注意，谢谢！
（cdmpview.tmp_wsj_Assess_4_program的信息每次合作运营部都会给到）
--=============
--节目观看次数均值	节目人均使用次数/当月上传节目数量
--cdmpview.tmp_wsj_Assess_4_program;
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_4_detail ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_4_detail AS
select bb.program_id business_id ,
	   count(distinct a.usernum_id)assess_4_num,
	   count(1)assess_4_cnt,
	   count(distinct bb.business_id)assess_4_pr_num
  FROM rptdata.fact_use_detail a
  join cdmpview.tmp_wsj_Assess_4_program bb
    on a.program_id=bb.business_id
 WHERE a.src_file_day between '20170501'and'20170531'
 group by bb.program_id;

--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_4 ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_4 AS
select a.business_id,(a.assess_4_cnt/a.assess_4_num)/a.assess_4_pr_num
from cdmpview.tmp_wsj_05_Assess_4_detail a;

--=============
--节目观看时长均值	节目人均使用时长/当月上传节目数量

--cdmpview.tmp_wsj_Assess_4_program;
--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_5_detail ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_5_detail AS
select  bb.program_id business_id,
	    count(distinct a.usernum_id)assess_5_num,
	    sum(duration_sec)assess_5_duration_sec,
	    count(distinct bb.business_id)assess_5_pr_num
  FROM rptdata.fact_use_detail a
  join cdmpview.tmp_wsj_Assess_4_program bb
    on a.program_id=bb.business_id
 WHERE a.src_file_day between '20170501'and'20170531'
 group by bb.program_id;

--DROP TABLE IF EXISTS cdmpview.tmp_wsj_05_Assess_5 ;
CREATE TABLE cdmpview.tmp_wsj_05_Assess_5 AS
select a.business_id,(a.assess_5_duration_sec/a.assess_5_num)/a.assess_5_pr_num
from cdmpview.tmp_wsj_05_Assess_5_detail a;