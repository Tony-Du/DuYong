package com.mganaly.mrtools.utils;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.mapred.cdmp_base_reducer;
import com.mganaly.cdmp.mapred.tdim_prod_id_mapper;

public class tdim_prod_id_tool  extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(tdim_prod_id_tool.class);
	
	
	protected int confJobParas (String[] args) throws IOException {
		
		int code = MR_SUCCESS;

		setCommConf();
		
		confCommJobParas();
		
		_mr_job.setMapperClass(tdim_prod_id_mapper.class);
		_mr_job.setReducerClass(cdmp_base_reducer.class);
		
		setDefaultPath(_mr_job);
		
		return code;
	}
}
