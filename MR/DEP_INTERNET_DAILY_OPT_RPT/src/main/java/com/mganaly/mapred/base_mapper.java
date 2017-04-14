package com.mganaly.mapred;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.KeyValPair;
import com.mganaly.cdmp.data.type.eInvalidCounters;

import org.apache.hadoop.mapreduce.Counter;

public class base_mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Log _LOG = LogFactory.getLog(base_mapper.class);
	
	protected String _cfgInCols;
	protected String _cfgKeyCols;
	protected String _cfgValCols;

	protected ArrayList<ColParser> _inColsList			= new ArrayList<ColParser>();
	protected ArrayList<ColParser> _keyParserList		= new ArrayList<ColParser>();
	protected ArrayList<ColParser> _valParserList		= new ArrayList<ColParser>();
	protected ArrayList<String> colsNames = null;
	
	protected cdmp_base_tbl _dataType 	= null;

	protected Counter _cntInvLen 	= null;

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		// Pre-process values
		String line = value.toString();

		KeyValPair kv = DataParserCfg.processKVData(line, _keyParserList, 
				_valParserList);


		if (null !=kv && kv.isValid()) {
			context.write(new Text(kv._key), new Text(kv._val));
		}

	} // protected void map
	

	protected void mapString(String [] items, Context context) 
			throws IOException, InterruptedException {

		
		KeyValPair kv = DataParserCfg.processKVData(items, _keyParserList, 
				_valParserList);
		
		if (null !=kv && kv.isValid()) {
			context.write(new Text(kv._key), new Text(kv._val));
		}
	}
	

	protected cdmp_base_tbl getDataType() {
		if (null == _dataType) {
			_dataType = new cdmp_base_tbl();
		}
		return _dataType;
	}
	
	
	protected void readConfig (Context context) throws IOException {
		Configuration conf = context.getConfiguration();

		_cfgKeyCols	= conf.get(DataParserCfg.MAP_KEY_CONF_NAME);
		if (null == _cfgKeyCols) {
			String err_msg = "ERROR: " + DataParserCfg.MAP_KEY_CONF_NAME + "is null.";
			throwIOException(err_msg);
		}

		_cfgValCols	= conf.get(DataParserCfg.MAP_VAL_CONF_NAME);
		if (null == _cfgValCols) {
			String err_msg = "ERROR: " + DataParserCfg.MAP_VAL_CONF_NAME + "is null.";
			throwIOException(err_msg);
		}
		

		_cfgInCols = conf.get(DataParserCfg.MAP_INPUT_COLS_CONF_NAME);
		
	}
	

	protected void genKVParsers(Context context) throws IOException {
		
		DataParserCfg.parseKVConf(_cfgKeyCols, _keyParserList, getDataType());
		DataParserCfg.genCounters(context, _keyParserList);
		DataParserCfg.setAsKey(_keyParserList);

		DataParserCfg.parseKVConf(_cfgValCols, _valParserList, getDataType());
		DataParserCfg.genCounters(context, _valParserList);
		

		if (null != _cfgInCols) {

			colsNames = DataParserCfg.parseColsName (_cfgInCols);
			DataParserCfg.genIndexsByName(colsNames, _keyParserList);
			DataParserCfg.genIndexsByName(colsNames, _valParserList);
		}

	} // genKVParsers


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		
		readConfig(context);
		
		genKVParsers(context);
		
		// Get len counter
		_cntInvLen = context.getCounter(eInvalidCounters.INV_LEN);

	} // void setup

	
	
	protected void throwIOException (String err_msg) throws IOException {
		_LOG.error(err_msg);
		throw new IOException (err_msg);		
	}

} //class cdmp_base_mapper
