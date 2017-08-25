


drop table qushupingtai.qspt_hzyykh_fcfj_use_in_order_user_rate;
create table qushupingtai.qspt_hzyykh_fcfj_use_in_order_user_rate (
company_id             string,
use_user_cnt           bigint,
in_order_user_cnt      bigint,
use_in_order_user_rate double   --有修改
)
partitioned by (src_file_month string);


drop table qushupingtai.qspt_hzyykh_fcfj_day_avg_visit_user;
create table qushupingtai.qspt_hzyykh_fcfj_day_avg_visit_user (
company_id             string,
day_avg_visit_user_cnt double  --有修改
)
partitioned by (src_file_month string);



drop table qushupingtai.qspt_hzyykh_fcfj_activ_visit;
create table qushupingtai.qspt_hzyykh_fcfj_activ_visit (
company_id             string,
day_avg_visit_user_cnt double,    --有修改
mon_visit_user_cnt     bigint,
activ_visit            double     --有修改
)
partitioned by (src_file_month string);