


set mapreduce.job.name=rptdata.fact_ugc_tpp_in_order_detail_daily${SRC_FILE_DAY};

set hive.groupby.skewindata=true;
set hive.optimize.skewjoin=true;
set hive.merge.mapredfiles = true;
insert overwrite table rptdata.fact_ugc_tpp_in_order_detail_daily partition (src_file_day='${SRC_FILE_DAY}')
select user_type_id,
	   net_type_id,
	   cp_id,
	   broadcast_type_id,
	   term_prod_id,
	   term_version_id,
	   chn_id,
	   city_id,
	   sub_busi_id,
	   user_id,
	   company_id
  from intdata.ugc_tpp_contract
 where substr(start_time, 1, 8) <='${SRC_FILE_DAY}' and substr(expire_time, 1, 8) >= '${SRC_FILE_DAY}'
 group by user_type_id, net_type_id, cp_id, broadcast_type_id, term_prod_id, term_version_id, chn_id, city_id, sub_busi_id, user_id, company_id;
