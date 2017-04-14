package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.mganaly.conf.DataIO;
import com.mganaly.conf.GlobalEv;
import com.mganaly.mapred.cdmp_td_filter_comparator_mapper;

public class cdmp_filter_comparator_tool extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(cdmp_filter_comparator_tool.class);
	
	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;
		
		setCommConf();
		
		confCommJobParas();

		_mr_job.setMapperClass(cdmp_td_filter_comparator_mapper.class);
		
		setDefaultPath(_mr_job);


		return code;
	} ////run(String[] args)

}
