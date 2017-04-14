package com.mganaly.utils;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.conf.GlobalEv;
import com.mganaly.mrtools.utils.MRCmdTool;

public abstract class MRTaskCmdTool extends MRCmdTool{
	
	private static final Log _LOG = LogFactory.getLog(MRCmdTool.class);

	@Override
	protected int confJobParas(String[] args) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	protected void setCommConf() {
		
		_conf = getConf();
    	_conf.set("mapreduce.output.textoutputformat.separator", 			ColParser.SEPRATOR);
		_conf.set("mapreduce.input.fileinputformat.input.dir.recursive", 	"true");
		_conf.set("mapreduce.job.priority", 								JobPriority.VERY_HIGH.toString());

		if (GlobalEv.DEBUG) {
			_conf.set("mapreduce.framework.name", 	"local");
			_conf.set("mapreduce.map.log.level", 	"DEBUG");
			_conf.set("mapreduce.reduce.log.level", 	"DEBUG");
		}
		
		setConf(_conf);
	}
	
	public Job getJob () {
		return null;
	}
	
	public int runAsyn (Configuration cfg, String[] args) throws Exception {
		return MR_ERROR;
		
	}
	
	public int run(String[] args) throws Exception {
		
		int code = MR_ERROR;
		
		return code;
	} ////run(String[] args)

}
