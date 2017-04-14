package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.mapred.cdmp_base_reducer;
import com.mganaly.cdmp.mapred.td_icms_drama_sin_rel_mapper;

public class td_icms_drama_sin_rel_tool extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(td_oms_program_d_tool.class);
	
	
	protected int confJobParas (String[] args) throws IOException {
		int code = MR_SUCCESS;

		setCommConf();
		
		confCommJobParas();
		
		_mr_job.setMapperClass(td_icms_drama_sin_rel_mapper.class);
		_mr_job.setReducerClass(cdmp_base_reducer.class);
		
		setDefaultPath(_mr_job);
		
		return code;
	}

}
