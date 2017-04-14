package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.conf.GlobalEv;

public abstract class join_base_tool extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(join_base_tool.class);

	@Override
	protected void setCommConf() throws IOException {
		_conf = getConf();

		String MAP_RIGHT_INPUT_COLS_CONF_NAME = _mapParams.get(PARAM_MAP_R_COLS);
		if (null != MAP_RIGHT_INPUT_COLS_CONF_NAME) {
			_conf.set(DataParserCfg.MAP_RIGHT_INPUT_COLS_CONF_NAME, MAP_RIGHT_INPUT_COLS_CONF_NAME);
			_LOG.info(DataParserCfg.MAP_RIGHT_INPUT_COLS_CONF_NAME 
					+ " : " + MAP_RIGHT_INPUT_COLS_CONF_NAME);
		}
		
		String TD_JOIN_LEFT_FLAG = getLeftFlage(_mapParams.get(PARAM_INPATH));
		_conf.set(DataParserCfg.TD_JOIN_LEFT_FLAG, TD_JOIN_LEFT_FLAG);
		_LOG.info(DataParserCfg.TD_JOIN_LEFT_FLAG + " : " + TD_JOIN_LEFT_FLAG);
		
		String TD_JOIN_MAP_RIGHT_KEY_CONF = _mapParams.get(PARAM_MAP_R_KEYS);
		_conf.set(DataParserCfg.TD_JOIN_MAP_RIGHT_KEY_CONF, TD_JOIN_MAP_RIGHT_KEY_CONF);
		_LOG.info(DataParserCfg.TD_JOIN_MAP_RIGHT_KEY_CONF 
				+ " : " + TD_JOIN_MAP_RIGHT_KEY_CONF);
		
		String TD_JOIN_MAP_RIGHT_VAL_CONF = _mapParams.get(PARAM_MAP_R_VALS);
		_conf.set(DataParserCfg.TD_JOIN_MAP_RIGHT_VAL_CONF, TD_JOIN_MAP_RIGHT_VAL_CONF);
		_LOG.info(DataParserCfg.TD_JOIN_MAP_RIGHT_VAL_CONF 
				+ " : " + TD_JOIN_MAP_RIGHT_VAL_CONF);
		
		String TD_JOIN_RED_RIGHT_KEY_CONF = _mapParams.get(PARAM_RED_R_KEYS);
		if (null != TD_JOIN_RED_RIGHT_KEY_CONF) {
			_conf.set(DataParserCfg.TD_JOIN_RED_RIGHT_KEY_CONF, TD_JOIN_RED_RIGHT_KEY_CONF);
			_LOG.info(DataParserCfg.TD_JOIN_RED_RIGHT_KEY_CONF 
					+ " : " + TD_JOIN_RED_RIGHT_KEY_CONF);
		}
		

		String TD_JOIN_RED_RIGHT_VAL_CONF = _mapParams.get(PARAM_RED_R_VALS);
		if (null != TD_JOIN_RED_RIGHT_VAL_CONF) {
			_conf.set(DataParserCfg.TD_JOIN_RED_RIGHT_VAL_CONF, TD_JOIN_RED_RIGHT_VAL_CONF);
			_LOG.info(DataParserCfg.TD_JOIN_RED_RIGHT_VAL_CONF 
					+ " : " + TD_JOIN_RED_RIGHT_VAL_CONF);
		}

		setConf(_conf);

		super.setCommConf();
	}
	
	
	protected void setDefaultPath(Job paraJob) throws IOException {

		String leftPath = _mapParams.get(PARAM_INPATH);
		String rightPath = _mapParams.get(PARAM_INPATH_R);
		
		if (null != _inStartDate) {
			leftPath +=  "/" + _inStartDate;
		}
		
		if (null != _inRStartDate) {
			rightPath +=  "/" + _inRStartDate;
		}
		
		
		addInputPath (paraJob, leftPath);
		addInputPath (paraJob, rightPath);
		
		String outPath = _mapParams.get(PARAM_OUTPATH);
		
		if (null != _outStartDate) outPath += "/" + _outStartDate;
		_LOG.info("outputPath : " + outPath);
		GlobalEv.checkAndRemove(_conf, outPath);
		FileOutputFormat.setOutputPath(paraJob, new Path(outPath));

	} // setDefaultPath
	
	protected static String getLeftFlage (String path) {
		
		String leftFlag = null;
		int lastSplash = path.length();
		
		lastSplash = path.lastIndexOf("/", path.length()-2);
		
		if (-1 != lastSplash) {
			leftFlag = path.substring(lastSplash+1);
		}
		
		return leftFlag;
	}
	
	
}
