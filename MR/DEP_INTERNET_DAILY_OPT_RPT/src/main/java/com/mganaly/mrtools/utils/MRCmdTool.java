package com.mganaly.mrtools.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.eInvalidCounters;
import com.mganaly.conf.DateHelper;
import com.mganaly.conf.GlobalEv;

/**
 * class MRCmdTool
 * 
 * 
 * About log: ////NONE, INFO, WARN, DEBUG, TRACE, and ALL
 * Configuration.set("mapreduce.map.log.level", "DEBUG");
 * Configuration.set("mapreduce.reduce.log.level", "TRACE");
 */
public abstract class MRCmdTool extends Configured implements Tool {

	private static final Log _LOG = LogFactory.getLog(MRCmdTool.class);

	protected Configuration _conf;
	protected Job _mr_job;

	protected cdmp_base_tbl _cdmp_data_type;
	long _red_ouput_records = -1;

	// protected String[] _inputPaths;
	//protected String _startDate = null;
	protected String _inStartDate = null;
	protected String _inRStartDate = null;
	protected String _outStartDate = null;
	// Parameters
	/****************************************************
	 * <arg>date=VALUE</arg> <arg>inpath=VALUE</arg> <arg>outpath=VALUE</arg>
	 * <arg>mapCols=VALUE</arg> <arg>mapKeys=VALUE</arg>
	 * <arg>mapVals=VALUE</arg> <arg>redKeys=VALUE</arg>
	 * <arg>redVals=VALUE</arg> <arg>redNum=VALUE</arg>
	 * <arg>mapRCols=VALUE</arg> <arg>mapRKeys=VALUE</arg>
	 * <arg>mapRVals=VALUE</arg>
	 */
	//public static final String PARAM_DATE = "date";
	protected static final String PARAM_IN_DATE = "inDate";
	protected static final String PARAM_INR_DATE = "inRDate";
	protected static final String PARAM_OUT_DATE = "outDate";
	protected static final String PARAM_START_DATE = "startDate";
	protected static final String PARAM_FROM_DATE = "fromDate";
	protected static final String PARAM_INPATH = "inpath";
	protected static final String PARAM_INPATH_R = "inpathR";
	protected static final String PARAM_OUTPATH = "outpath";
	protected static final String PARAM_MAP_COLS = "mapCols";
	protected static final String PARAM_MAP_KEYS = "mapKeys";
	protected static final String PARAM_MAP_VALS = "mapVals";
	protected static final String PARAM_RED_KEYS = "redKeys";
	protected static final String PARAM_RED_VALS = "redVals";
	protected static final String PARAM_RED_NUM = "redNum";
	protected static final String PARAM_MAP_R_COLS = "mapRCols";
	protected static final String PARAM_MAP_R_KEYS = "mapRKeys";
	protected static final String PARAM_MAP_R_VALS = "mapRVals";
	protected static final String PARAM_RED_R_KEYS = "redRKeys";
	protected static final String PARAM_RED_R_VALS = "redRVals";
	protected static final String PARAM_TABLE_NAME = "tableName";
	protected static final String PARAM_TABLE_COLS_NUM = "tableColNum";
	protected static final String PARAM_SOURCE_SEPRATOR = "srcSep";
	protected static final String PARAM_TARGET_SEPRATOR = "tgtSep";
	protected static final String PARAM_RESERVE = "RESERVE";

	// public static final String PARAM_VAL_UNINIT = "valUninit";
	public static final String PARAM_PATH_SEP_SPLIT = "\\x2c"; // , comma
	public static final String PARAM_SEP_SPLIT = "\\x3d"; // = equal
	protected HashMap<String, String> _mapParams = new HashMap<String, String>();

	// Command execute flag
	protected final int MR_SUCCESS = 0;
	protected final int MR_ERROR = 1;
	protected final int MR_SKIP = 2;

	// Counters
	Counters _counters = null;
	long _cnt_total = 0;
	String _counter_time = null;

	protected abstract int confJobParas(String[] args) throws IOException;

	public MRCmdTool() {
	}

	protected void setCommConf() throws IOException {
		
		// Configuration
		_conf = getConf();
		_conf.set("mapreduce.output.textoutputformat.separator", ColParser.SEPRATOR);
		_conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
		_conf.set("mapreduce.job.priority", JobPriority.VERY_HIGH.toString());

		if (GlobalEv.DEBUG) {
			_conf.set("mapreduce.framework.name", "local");
			_conf.set("mapreduce.map.log.level", "DEBUG");
			_conf.set("mapreduce.reduce.log.level", "DEBUG");
		}

		String MAP_INPUT_COLS_CONF_NAME = _mapParams.get(PARAM_MAP_COLS);
		if (null != MAP_INPUT_COLS_CONF_NAME) {
			_conf.set(DataParserCfg.MAP_INPUT_COLS_CONF_NAME, MAP_INPUT_COLS_CONF_NAME);
			_LOG.info(DataParserCfg.MAP_INPUT_COLS_CONF_NAME + " : " + MAP_INPUT_COLS_CONF_NAME);
		}

		String MAP_KEY_CONF_NAME = _mapParams.get(PARAM_MAP_KEYS);
		if (null != MAP_KEY_CONF_NAME) {
			_conf.set(DataParserCfg.MAP_KEY_CONF_NAME, MAP_KEY_CONF_NAME);
			_LOG.info(DataParserCfg.MAP_KEY_CONF_NAME + " : " + MAP_KEY_CONF_NAME);
			_LOG.info(DataParserCfg.MAP_KEY_CONF_NAME + " len : " 
			+ MAP_KEY_CONF_NAME.split(DataParserCfg.SEPRATOR).length);
		}
		
		String RED_KEY_CONF_NAME = _mapParams.get(PARAM_RED_KEYS);
		if (null != RED_KEY_CONF_NAME) {
			_conf.set(DataParserCfg.RED_KEY_CONF_NAME, RED_KEY_CONF_NAME);
			_LOG.info(DataParserCfg.RED_KEY_CONF_NAME + " : " + RED_KEY_CONF_NAME);
		}

		String MAP_VAL_CONF_NAME = _mapParams.get(PARAM_MAP_VALS);
		if (null != MAP_VAL_CONF_NAME) {
			_conf.set(DataParserCfg.MAP_VAL_CONF_NAME, MAP_VAL_CONF_NAME);
			_LOG.info(DataParserCfg.MAP_VAL_CONF_NAME + " : " + MAP_VAL_CONF_NAME);
		}

		String RED_VAL_CONF_NAME = _mapParams.get(PARAM_RED_VALS);
		if (null != RED_VAL_CONF_NAME) {
			_conf.set(DataParserCfg.RED_VAL_CONF_NAME, RED_VAL_CONF_NAME);
			_LOG.info(DataParserCfg.RED_VAL_CONF_NAME + " : " + RED_VAL_CONF_NAME);
		}

		setConf(_conf);
	}

	protected boolean parseParams(String[] args) throws IOException, ParseException {
		boolean bParse = true;

		for (String arg : args) {
			String[] paraTypeVals = arg.trim().split(PARAM_SEP_SPLIT);

			if (paraTypeVals.length != 2) {
				throw new IOException("ERROR: Parameter " + arg + " should be [TYPE:VALUE]");
			}
			
			for (int i = 0; i < paraTypeVals.length; ++i) {
				paraTypeVals[i] = paraTypeVals[i].trim();
			}

			_mapParams.put(paraTypeVals[0], paraTypeVals[1]);
			_LOG.info("Parameter is " + paraTypeVals[0] + "\t=\t" + paraTypeVals[1]);
		}

		// Set default reduce number
		String strRedNum = _mapParams.get(PARAM_RED_NUM);
		if (null == strRedNum) {
			_mapParams.put(PARAM_RED_NUM, String.valueOf(GlobalEv.REDUCE_NUM_NORMAL));
		}

		parseDate();

		return bParse;
	}

	public void parseDate() throws ParseException {
		
		_conf = getConf();

		String inDate = _mapParams.get(PARAM_IN_DATE);
		
		if (null != inDate) {
			 _inStartDate = DateHelper.getFMTyyyyMMdd(inDate);
			_LOG.info("_inStartDate:\t" + _inStartDate);
		}
		

		String inRDate = _mapParams.get(PARAM_INR_DATE);
		
		if (null != inRDate) {
			_inRStartDate = DateHelper.getFMTyyyyMMdd(inRDate);
			_LOG.info("_inRStartDate:\t" + _inRStartDate);
		}
		
		
		String outDate = _mapParams.get(PARAM_OUT_DATE);
		
		if (null != outDate) {
			_outStartDate = DateHelper.getFMTyyyyMMdd(outDate);
			_LOG.info("_inStartDate:\t" + _outStartDate);
		}

		
		String startDate = _mapParams.get(PARAM_START_DATE);
		
		if (null != startDate) {
			startDate = DateHelper.getFMTyyyyMMdd(startDate);
			
			_LOG.info("startDate:\t" + startDate);
			_conf.set(DataParserCfg.MR_START_DAY, startDate); 
		}
		

		String fromDate = _mapParams.get(PARAM_FROM_DATE);
		
		if (null != fromDate) {
			
			 fromDate = DateHelper.getFMTyyyyMMdd(fromDate);
			_LOG.info("fromDate:\t" + fromDate);

			_conf.set(DataParserCfg.MR_FROM_DAY, fromDate); 
		}
		
		setConf(_conf);
	}
	

	protected void addInputPath (Job paraJob, String inputPathDir) throws IOException {
		
		_LOG.info("addInputPath =\t" + inputPathDir);

		String[] inputPaths = inputPathDir.trim().split(PARAM_PATH_SEP_SPLIT);

		// Set Input path
		FileSystem fs = FileSystem.get(_conf);
		if (null != inputPaths && 0 != inputPaths.length) {
			for (String inpathlist : inputPaths) {
				if (inpathlist.isEmpty())
					continue;
				
				inpathlist = inpathlist.trim();
				
				_LOG.info("inpathlist =\t" + inpathlist);
				FileStatus[] status = fs.globStatus(new Path(inpathlist.trim()));
				Path[] listedPaths = FileUtil.stat2Paths(status);
				for (Path inpath : listedPaths) {
					if (fs.exists(inpath)) {
						_LOG.info("inputPath =\t" + inpath.toString());
						FileInputFormat.addInputPath(paraJob, inpath);
					} else {
						_LOG.error("Error inputPath =\t" + inpath.toString());
					}
				} // for (Path inpath
			} // for (String inpathlist
		} // if (null != inputPaths
	}

	protected void setDefaultPath(Job paraJob) throws IOException {

		String inputPathDir = _mapParams.get(PARAM_INPATH);
		

		if (null != _inStartDate) {
			inputPathDir += "/" + _inStartDate;
		}
		
		addInputPath (paraJob, inputPathDir);
		
		// Set output path
		String outPath = _mapParams.get(PARAM_OUTPATH);
		if (null != outPath) {

			if (null != _outStartDate) outPath += "/" + _outStartDate;
			_LOG.info("outputPath : " + outPath);
			
			// check and Remove output file
			GlobalEv.checkAndRemove(_conf, outPath);	
			FileOutputFormat.setOutputPath(paraJob, new Path(outPath));
		}

	} // setDefaultPath

	protected String getCurTime() {
		// counter time stamp
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		return df.format(new Date());
	}

	protected void outputCounters() throws IOException {

		_counters = _mr_job.getCounters();
		_cnt_total = _counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

		_LOG.info("Total = " + _cnt_total);

		String counters_report = "";
		eInvalidCounters[] counters = eInvalidCounters.values();
		for (eInvalidCounters counter : counters) {

			long counter_num = _counters.findCounter(counter).getValue();			
			if (counter_num > 0) {
				counters_report = String.format(counter.name() + "\t%.2f%%\n ", 
						(float) (counter_num * 100.0 / _cnt_total));
				
				_LOG.info(counters_report);
			}
		}

	}

	public Job getJob() {
		return _mr_job;
	}

	public int runAsyn(Configuration cfg, String[] args) throws Exception {

		setConf(cfg);

		int code = MR_SUCCESS;
		if (MR_SUCCESS == (code = confJobParas(args))) {
			_mr_job.submit();
		}

		return code;

	}

	public int run(String[] args) throws Exception {

		parseParams(args);

		int code = confJobParas(args);

		if (MR_SUCCESS == code) {

			code = _mr_job.waitForCompletion(true) ? 0 : 1;
			_red_ouput_records = _mr_job.getCounters()
					.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue();
			_LOG.info("REDUCE_OUTPUT_RECORDS\t" + _red_ouput_records);

			if (0 >= _red_ouput_records) {
				code = MR_ERROR;
				_LOG.error("*********************************************************");
				_LOG.error("Reduce Analysis empty...");
			}

			outputCounters();

		}

		return code;
	} //// run(String[] args)
	
	
	protected void confCommJobParas() throws IOException {

		_conf = getConf();
		_mr_job = Job.getInstance(_conf);
		_mr_job.setJobName(getClass().getSimpleName() + getCurTime());
		_mr_job.setJarByClass(getClass());

		_mr_job.setNumReduceTasks(Integer.parseInt(_mapParams.get(PARAM_RED_NUM)));
		
		_mr_job.setMapOutputKeyClass(Text.class);  
		_mr_job.setMapOutputValueClass(Text.class);
        
		_mr_job.setOutputKeyClass(Text.class);
		_mr_job.setOutputValueClass(Text.class);
		
	}

} // class MRCmdTool extends
