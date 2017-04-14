package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobPriority;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.conf.GlobalEv;
import com.mganaly.mapred.prep_seprate_mapper;
import com.mganaly.mapred.prep_seprate_reducer;

public class prep_seperate_tool extends MRCmdTool{
	

	private static final Log _LOG = LogFactory.getLog(prep_seperate_tool.class);
	
	
	protected int confJobParas (String[] args) throws IOException {
		
		int code = MR_SUCCESS;

		setCommConf();
		
		confCommJobParas();
		
		_mr_job.setMapperClass(prep_seprate_mapper.class);
		_mr_job.setReducerClass(prep_seprate_reducer.class);
		
		setDefaultPath(_mr_job);
		
		return code;
	}
	
	@Override
	protected void setCommConf() throws IOException {
		super.setCommConf();		

		_conf = getConf();

		
		String SEPRATOR_SOURCE = _mapParams.get(PARAM_SOURCE_SEPRATOR);
		if (null != SEPRATOR_SOURCE) {
			if (0 == SEPRATOR_SOURCE.compareToIgnoreCase("TAB")) {
				SEPRATOR_SOURCE = "\t";
			}
			_conf.set(DataParserCfg.SEPRATOR_SOURCE, SEPRATOR_SOURCE);
			_LOG.info(DataParserCfg.SEPRATOR_SOURCE + " : " + SEPRATOR_SOURCE);
		}
		

		String SEPRATOR_TARGET = _mapParams.get(PARAM_TARGET_SEPRATOR);
		if (null != SEPRATOR_SOURCE) {
			if (0 == SEPRATOR_TARGET.compareToIgnoreCase("TAB")) {
				SEPRATOR_TARGET = "\t";
			}
			else {
				_conf.set("mapreduce.output.textoutputformat.separator", SEPRATOR_TARGET);
			}
			_conf.set(DataParserCfg.SEPRATOR_TARGET, SEPRATOR_TARGET);
			_LOG.info(DataParserCfg.SEPRATOR_TARGET + " : " + SEPRATOR_TARGET);
		}
		
		
		setConf(_conf);
	}
	

}
