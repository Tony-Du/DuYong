package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.mganaly.conf.DataIO;
import com.mganaly.conf.GlobalEv;
import com.mganaly.mapred.cdmp_td_merge_mapper;
import com.mganaly.mapred.cdmp_td_merge_reducer;

public class merge_tool extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(merge_tool.class);

	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		confCommJobParas();


		return MR_ERROR;
	} ////run(String[] args)


}
