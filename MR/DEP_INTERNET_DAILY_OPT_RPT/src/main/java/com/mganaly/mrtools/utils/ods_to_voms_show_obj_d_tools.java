package com.mganaly.mrtools.utils;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.mapred.cdmp_base_reducer;
import com.mganaly.cdmp.mapred.ods_to_voms_show_obj_d_mapper;

public class ods_to_voms_show_obj_d_tools extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(ods_to_voms_show_obj_d_tools.class);
	
	
	protected int confJobParas (String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;

		setCommConf();
		
		confCommJobParas();
		
		_mr_job.setMapperClass(ods_to_voms_show_obj_d_mapper.class);
		_mr_job.setReducerClass(cdmp_base_reducer.class);
		
		setDefaultPath(_mr_job);
		
		return code;
	}
	
}
