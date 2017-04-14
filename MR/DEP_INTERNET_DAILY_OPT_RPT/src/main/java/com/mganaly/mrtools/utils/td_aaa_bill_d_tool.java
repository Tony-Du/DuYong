package com.mganaly.mrtools.utils;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import com.mganaly.cdmp.mapred.cdmp_base_reducer;
import com.mganaly.cdmp.mapred.td_aaa_bill_d_mapper;

public class td_aaa_bill_d_tool extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(td_aaa_bill_d_tool.class);

	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;

		setCommConf();
		
		confCommJobParas();

		_mr_job.setMapperClass(td_aaa_bill_d_mapper.class);
		_mr_job.setReducerClass(cdmp_base_reducer.class);
		
		setDefaultPath(_mr_job);
		
		return code;
	}
	

	public static void main(String[] args) throws Exception
    {
        int exitCode = ToolRunner.run(new td_aaa_bill_d_tool(), args);
        System.exit(exitCode);
    }


}
