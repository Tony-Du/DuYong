package com.mganaly.mrtools.utils;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.cdmp.mapred.cdmp_len_checker_mapper;

public class cdmp_format_check_tool extends MRCmdTool{
	
	private static final Log _LOG = LogFactory.getLog(td_aaa_bill_d_tool.class);


	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;

		setCommConf();
		
		confCommJobParas();
		_mr_job.setMapperClass(cdmp_len_checker_mapper.class);
		//_mr_job.setCombinerClass(cdmp_format_check_reducer.class);
		//_mr_job.setReducerClass(cdmp_format_check_reducer.class);

		setDefaultPath(_mr_job);
		
		return code;
	}

}
