package com.mganaly.conf;

public class DataIO {
	
	//-------------------------------------------------//
	// Input
	public final static String inputRoot = "testly/input";
	public final static String in_cdmp = "/user/hadoop/public/cdmp";
	
	
	//-------------------------------------------------//
	// cdmp	
	public final static String cdmp_td_aaa_bill_d 					= "/td_aaa_bill_d";
	public final static String cdmp_td_aaa_tourist_bill_d 			= "/td_aaa_tourist_bill_d";
	public final static String td_aaa_merge_bill_d 					= "/td_aaa_merge_bill_d";
	public final static String cdmp_td_pub_visit_log_d 				= "/td_pub_visit_log_d";
	public final static String cdmp_td_pub_visit_log_wd_tourist_d 	= "/td_pub_tourist_visit_log_d";
	public final static String cdmp_ods_to_voms_show_obj_d			= "/to_voms_show_obj_d";
	public final static String tdim_prod_id							= "/tdim_prod_id";
	
	
	public final static String td_join_chn_usr 				= "/td_join_chn_usr";
	public final static String td_sum_d 					= "/td_sum_d";
	public final static String td_sort_d 					= "/td_sort_d";
	public final static String td_filter_comparator_d 		= "/td_filter_comparator_d";
	public final static String td_select_d 					= "/td_select_d";
	
	//-------------------------------------------------//
	// preprocess
	public final static String out_preprocess	= "/out_preprocess";

	//-------------------------------------------------//
	/// Output
	public final static String out_pub = "/user/hadoop/public";
	
	//-------------------------------------------------//
	// output root
	public final static String out_migu_analysis 	= "/migu_analyo_ly";
	public final static String out_result 			= "/user/hadoop/public/migu_analyo_ly/output";
	public final static String out_tmp 				= "/user/hadoop/public/migu_analyo_ly/out_temp";
	
	//-------------------------------------------------//
	/// dimensionality
	public final static String out_user_check = "/user_check";
	public final static String data_format_check = "/data_format_check";
	
	//-------------------------------------------------//	
	/// classify
	public final static String out_static 	= "/static";
	public final static String out_join		= "/join";
	public final static String out_format 	= "/format";
	public final static String td_red_join 	= "/td_red_join";
	public final static String td_map_join 	= "/td_map_join";
}
