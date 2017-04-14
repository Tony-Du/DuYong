package com.mganaly.cdmp.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.mapred.base_mapper;

public class cdmp_len_checker_mapper  extends base_mapper {
	
	private static final Log _LOG = LogFactory.getLog(td_aaa_bill_d_mapper.class);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	} // void setup

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String [] items = null;
		
		String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
		
		if (pathName.contains("td_aaa_account_log_d")) {			
	        items = ColParser.splitString(line, 9);			
		}
		else if (pathName.contains("td_aaa_account_log_h")) {
			items = ColParser.splitString(line, 9);
		}
		else if (pathName.contains("td_aaa_bill_d")) {
			items = ColParser.splitString(line, 42);			
		}
		else if (pathName.contains("td_aaa_bill_h")) {
			items = ColParser.splitString(line, 43);			
		}
		else if (pathName.contains("td_aaa_bill_sup_d")) {
			items = ColParser.splitString(line, 31);			
		}
		else if (pathName.contains("td_aaa_calculate_order_d")) {
			items = ColParser.splitString(line, 23);
		}
		else if (pathName.contains("td_aaa_deal_log_d")) {
			items = ColParser.splitString(line, 19);			
		}
		else if (pathName.contains("td_aaa_order_d")) {
			items = ColParser.splitString(line, 24);			
		}
		else if (pathName.contains("td_aaa_order_log_d")) {
			items = ColParser.splitString(line, 30);
		}
		else if (pathName.contains("td_aaa_order_log_h")) {
			items = ColParser.splitString(line, 29);
		}
		else if (pathName.contains("td_aaa_reg_log_d")) {
			items = ColParser.splitString(line, 5);
		}
		else if (pathName.contains("td_aaa_tourist_bill_d")) {
			items = ColParser.splitString(line, 38);			
		}
		else if (pathName.contains("td_aaa_tourist_bill_h")) {
			items = ColParser.splitString(line, 44);			
		}
		else if (pathName.contains("td_aaa_user_register_info_d")) {
			items = ColParser.splitString(line, 15);
		}
		else if (pathName.contains("td_cms_content_attr_d")) {
			items = ColParser.splitString(line, 4);
		}
		else if (pathName.contains("td_cms_content_class_d")) {
			items = ColParser.splitString(line, 8);
		}
		else if (pathName.contains("td_cms_content_d")) {
			items = ColParser.splitString(line, 28);			
		}
		else if (pathName.contains("td_cms_content_file_d")) {
			items = ColParser.splitString(line, 9);			
		}
		else if (pathName.contains("td_cms_copyright_d")) {
			items = ColParser.splitString(line, 5);			
		}
		else if (pathName.contains("td_cms_cp_d")) {
			items = ColParser.splitString(line, 5);
		}
		else if (pathName.contains("td_complain_wkodr_detail_d")) {
			items = ColParser.splitString(line, 24);
		}
		else if (pathName.contains("td_external_links_log_d")) {
			items = ColParser.splitString(line, 7);
		}
		else if (pathName.contains("td_figure_code_validate_log_d")) {
			items = ColParser.splitString(line, 12);
		}
		else if (pathName.contains("td_function_oper_log_d")) {
			items = ColParser.splitString(line, 7);			
		}
		else if (pathName.contains("td_icms_drama_sin_rel")) {
			items = ColParser.splitString(line, 3);			
		}
		else if (pathName.contains("td_icms_ncpid_d")) {
			items = ColParser.splitString(line, 3);
		}
		else if (pathName.contains("td_mms_guide_send_log_d")) {
			items = ColParser.splitString(line, 8);
		}
		else if (pathName.contains("td_mss_code_send_log_d")) {
			items = ColParser.splitString(line, 13);
		}
		else if (pathName.contains("td_mss_code_validate_log_d")) {
			items = ColParser.splitString(line, 14);
		}
		else if (pathName.contains("td_node_info_d")) {
			items = ColParser.splitString(line, 18);
		}
		else if (pathName.contains("td_oms_content_collection_d")) {
			items = ColParser.splitString(line, 8);
		}
		else if (pathName.contains("td_oms_node_d")) {
			items = ColParser.splitString(line, 10);
		}
		else if (pathName.contains("td_oms_node_program_d")) {
			items = ColParser.splitString(line, 4);
		}
		else if (pathName.contains("td_oms_product_info_desc_d")) {
			items = ColParser.splitString(line, 3);
		}
		else if (pathName.contains("td_oms_term_d")) {
			items = ColParser.splitString(line, 19);
		}
		else if (pathName.contains("td_pms_product_d")) {
			items = ColParser.splitString(line, 18);
		}
		else if (pathName.contains("td_poms_productpackage_info_d")) {
			items = ColParser.splitString(line, 8);
		}
		else if (pathName.contains("td_poms_productpkg_relation_d")) {
			items = ColParser.splitString(line, 4);
		}
		else if (pathName.contains("td_program_info_d")) {
			items = ColParser.splitString(line, 23);
		}
		else if (pathName.contains("td_pswd_back_log_d")) {
			items = ColParser.splitString(line, 15);
		}
		else if (pathName.contains("td_pub_search_log_d")) {
			items = ColParser.splitString(line, 7);
		}
		else if (pathName.contains("td_pub_suggest_content_d")) {
			items = ColParser.splitString(line, 6);
		}
		else if (pathName.contains("td_pub_tourist_visit_log_d")) {
			items = ColParser.splitString(line, 37);
		}
		else if (pathName.contains("td_pub_visit_log_d")) {
			items = ColParser.splitString(line, 38);			
		}
		else if (pathName.contains("td_pub_visit_log_h")) {
			items = ColParser.splitString(line, 29);
		}
		else if (pathName.contains("td_pub_visit_log_wd_tourist_d")) {
			items = ColParser.splitString(line, 37);
		}
		else if (pathName.contains("td_static_reg_log_d")) {
			items = ColParser.splitString(line, 16);			
		}
		else if (pathName.contains("td_tourist_login_log_d")) {
			items = ColParser.splitString(line, 18);			
		}
		else if (pathName.contains("td_two_complaint_order_d")) {
			items = ColParser.splitString(line, 21);			
		}
		else if (pathName.contains("td_two_complaint_use_d")) {
			items = ColParser.splitString(line, 28);
		}
		else if (pathName.contains("td_voms_locanode_d")) {
			items = ColParser.splitString(line, 14);
		}
		else if (pathName.contains("td_wap_order_touch_d")) {
			items = ColParser.splitString(line, 17);
		}
		else if (pathName.contains("td_wap_portal_login_log_d")) {
			items = ColParser.splitString(line, 5);
		}
		else if (pathName.contains("td_wap_term_down_log_d")) {
			items = ColParser.splitString(line, 10);
		}
		else if (pathName.contains("td_wap_term_upgrade_log_d")) {
			items = ColParser.splitString(line, 11);
		}
		else if (pathName.contains("td_wap_use_log_d")) {
			items = ColParser.splitString(line, 8);
		}
		else if (pathName.contains("td_wap_use_log_h")) {
			items = ColParser.splitString(line, 8);
		}
		else {
			
		}

		String mapKey = pathName;
		String mapVal = "1" + ColParser.SEPRATOR;
		
		if (null == items) {
			mapVal += "1";
		}
		else {
			mapVal += "0";
		}
		
		//_LOG.info("mapKey = \t" + mapKey);
		//_LOG.info("mapVal = \t" + mapVal);
		context.write(new Text(mapKey), new Text(mapVal));
	} // protected void map

		

	@Override
	protected cdmp_base_tbl getDataType() {
		if (null == _dataType) {
			_dataType = new cdmp_base_tbl();
		}
		return _dataType;
	}

}
