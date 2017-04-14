package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.mapred.outer_join_mapper;
import com.mganaly.mapred.outer_join_reducer;

public class outer_join_tool extends join_base_tool {
	
	private static final Log _LOG = LogFactory.getLog(outer_join_tool.class);
		
	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;
		
		setCommConf();
		
		confCommJobParas();
		
		_mr_job.setMapperClass(outer_join_mapper.class);
		_mr_job.setReducerClass(outer_join_reducer.class);

		setDefaultPath(_mr_job);

		return code;
	}	//run(String[] args) 

} //class outer_join_tool
