

create table qushupingtai.qspt_hzyykh_fcfj_accu_add_revenue (
company_id       string,
accu_add_revenue bigint
)
partitioned by (src_file_month string);


create table qushupingtai.qspt_hzyykh_fcfj_use_user (
company_id   string,
use_user_cnt bigint
)
partitioned by (src_file_month string);


create table qushupingtai.qspt_hzyykh_fcfj_in_order_user (
company_id        string,
in_order_user_cnt bigint
)
partitioned by (src_file_month string);


create table qushupingtai.qspt_hzyykh_fcfj_use_in_order_user_rate (
company_id             string,
use_user_cnt           bigint,
in_order_user_cnt      bigint,
use_in_order_user_rate double   --有修改
)
partitioned by (src_file_month string);


create table qushupingtai.qspt_hzyykh_fcfj_mon_visit_user (
company_id         string,
mon_visit_user_cnt bigint
)
partitioned by (src_file_month string);



create table qushupingtai.qspt_hzyykh_fcfj_day_avg_visit_user (
company_id             string,
day_avg_visit_user_cnt double  --有修改
)
partitioned by (src_file_month string);



create table qushupingtai.qspt_hzyykh_fcfj_activ_visit (
company_id             string,
day_avg_visit_user_cnt double,    --有修改
mon_visit_user_cnt     bigint,
activ_visit            double     --有修改
)
partitioned by (src_file_month string);



