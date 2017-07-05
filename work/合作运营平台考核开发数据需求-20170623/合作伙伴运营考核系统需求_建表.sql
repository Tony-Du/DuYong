

create table cdmpview.tmp_dy_05_check_1_2 (
statis_month string,
business_id  string,
usernum_id   string,
flow_kb      bigint,
duration_sec bigint
)


create table cdmpview.tmp_dy_05_check_3 (
statis_month    string,
business_id     string,
in_3_check_num  bigint,
all_check_3_num bigint
)

create table cdmpview.tmp_dy_05_check_4 (
statis_month    string,
business_id     string,
in_4_check_num  bigint,   --in_4_check_num --should be in_3_check_num
all_check_4_num bigint    --all_check_4_num
)

----------------------------------------------
create table cdmpview.tmp_dy_05_assess_1_add_revenue (
statis_month string,
business_id string,
accu_add_revenue bigint
)
partitioned by (src_file_month string)

create table cdmpview.tmp_dy_05_Assess_2_u (
statis_month  string,
business_id   string,
use_user_cnt  bigint
)

create table cdmpview.tmp_dy_05_Assess_2_order (
statis_month  string,
business_id   string,
in_order_user_cnt  bigint
)

create table cdmpview.tmp_dy_05_Assess_2 (
statis_month           string,
business_id            string,
use_user_cnt           bigint,
in_order_user_cnt      bigint,
use_in_order_user_rate double
)

create table cdmpview.tmp_dy_05_Assess_3_d (
statis_month           string,
business_id            string,
day_avg_visit_user_cnt bigint
)


create table cdmpview.tmp_dy_05_Assess_3_m (
statis_month           string,
business_id            string,
login_visit_user_cnt   bigint,
visit_user_cnt         bigint
)

create table cdmpview.tmp_dy_05_Assess_4 (
statis_month       string,
business_id        string,
prog_watch_avg_cnt bigint       --节目观看次数均值
)


create table cdmpview.tmp_dy_05_Assess_5 (
statis_month           string,
business_id            string,
prog_watch_avg_dur_sec bigint   --节目观看时长均值 
)










