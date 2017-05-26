
create table app.user_visit_detail_sx0290_monthly (
phone_number   string,
visit_date     string,
busi_prod_name string
)
partitioned by (src_file_month string)
row format delimited fields terminated by '31'
stored as textfile;