package com.mganaly.mrtools.utils;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.cdmp.mapred.cdmp_base_reducer;
import com.mganaly.cdmp.mapred.td_pub_visit_log_d_mapper;

public class td_pub_visit_log_d_tool  extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(td_pub_visit_log_d_tool.class);
	
	@Override
	protected int confJobParas (String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;

		setCommConf();
		
		confCommJobParas();
		
		_mr_job.setMapperClass(td_pub_visit_log_d_mapper.class);
		_mr_job.setReducerClass(cdmp_base_reducer.class);
		
		setDefaultPath(_mr_job);
		
		return code;
	}
	

}
