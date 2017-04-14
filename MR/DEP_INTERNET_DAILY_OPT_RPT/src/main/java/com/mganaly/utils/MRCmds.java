package com.mganaly.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class MRCmds {
	
	private static final Log _LOG = LogFactory.getLog(MRCmds.class);

	public static enum eType {
		Cluster
		, cmd_del_path
		, cdmp_format_check_tool
		, cdmp_filter_comparator_tool
		, prep_seperate_tool
		
		, ods_to_voms_show_obj_d_tool
		, td_aaa_bill_d_tool
		, td_aaa_tourist_bill_d_tool
		, td_pub_visit_log_d_tool
		, td_pub_visit_log_wd_tourist_d_tool
		, td_oms_program_d_tool
		, tdim_prod_id_tool
		, td_icms_drama_sin_rel_tool
		, td_cms_content_attr_d_tool
		, td_cms_content_class_d_tool
		, td_cms_content_d_tool
		, td_userid_usernum_tool
		
		, merge_tool
		, map_join_tool
		, inner_join_tool
		, left_join_tool
		, outer_join_tool
		, mr_general_tool
		, select_tool
		, mysql_table_submit
		; 
	}

	
	public MRCmds()
	{
	}
	
	
	public void printTips()
	{
		_LOG.info("COMMAND PARAMETERS:-------------------");
		
		eType[] ecmds = eType.values();
		for (eType ecmd : ecmds) {
			_LOG.info(ecmd.name());
		}
		_LOG.info("--------------------------------------");
	} //void printTips
	
} //MRToolCmd
