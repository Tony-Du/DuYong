package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.mapred.left_join_mapper;
import com.mganaly.mapred.left_join_reducer;

public class left_join_tool extends join_base_tool {
	
	private static final Log _LOG = LogFactory.getLog(inner_join_tool.class);
		
	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;
		
		setCommConf();
		
		confCommJobParas();
		
		_mr_job.setMapperClass(left_join_mapper.class);
		_mr_job.setReducerClass(left_join_reducer.class);

		setDefaultPath(_mr_job);

		return code;
	}	//run(String[] args) 
}
