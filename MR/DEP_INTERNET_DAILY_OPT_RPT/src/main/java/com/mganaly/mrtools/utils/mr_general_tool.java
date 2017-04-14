package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import com.mganaly.conf.DataIO;
import com.mganaly.conf.GlobalEv;
import com.mganaly.mapred.mr_general_mapper;
import com.mganaly.mapred.mr_general_reducer;

public class mr_general_tool  extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(mr_general_tool.class);


	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;
		
		setCommConf();
		
		confCommJobParas();

		_mr_job.setMapperClass(mr_general_mapper.class);
		_mr_job.setReducerClass(mr_general_reducer.class);
		
		setDefaultPath(_mr_job);

		return code;
	} ////run(String[] args)
	
}
