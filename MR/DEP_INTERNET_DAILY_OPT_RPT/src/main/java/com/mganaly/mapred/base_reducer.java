package com.mganaly.mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;

/**
 * 
 * class cdmp_base_reducer
 *
 */
public class base_reducer extends Reducer<Text, Text, Text, Text> {
	
	private static final Log _LOG = LogFactory.getLog(base_reducer.class);

	private String _cfgKeyCols;
	protected String _cfgValCols;
	protected int 	_szKeyCols = 0;
	
	private String _cfgMapKeyCols;
	private String _cfgMapValCols;
	
	private boolean _bNoReduceMode = false;

	protected ArrayList<ColParser> _kvParserList = new ArrayList<ColParser>();
	
	protected cdmp_base_tbl _data_type 		= null;
	
	
	protected cdmp_base_tbl getDataType() {
		if (null == _data_type) {
			_data_type = new cdmp_base_tbl();
		}
		return _data_type;
	}
	
	protected void readConfig (Context context) {
		Configuration conf = context.getConfiguration();
		

		_cfgMapKeyCols	= conf.get(DataParserCfg.MAP_KEY_CONF_NAME).trim();
		_cfgMapValCols	= conf.get(DataParserCfg.MAP_VAL_CONF_NAME).trim();
		

		_cfgKeyCols = conf.get(DataParserCfg.RED_KEY_CONF_NAME);
		_cfgValCols = conf.get(DataParserCfg.RED_VAL_CONF_NAME);
		if (null == _cfgKeyCols && null == _cfgValCols) {
			_bNoReduceMode = true;
		}
		
		if (null == _cfgKeyCols || _cfgKeyCols.isEmpty()) {
			_cfgKeyCols = _cfgMapKeyCols;			
		}
		
		_cfgKeyCols = _cfgKeyCols.trim();
		
		_szKeyCols = _cfgKeyCols.split(DataParserCfg.SEPRATOR).length;
		
		if (null == _cfgValCols || _cfgValCols.isEmpty()) {
			_cfgValCols = _cfgMapValCols;
		}
		_cfgValCols = _cfgValCols.trim();
		
	}
	
	protected void genKVParsers(Context context) throws IOException {

		// Gen kv parser list
		String cfgKVCols = _cfgKeyCols.trim() + DataParserCfg.SEPRATOR + _cfgValCols.trim();
		DataParserCfg.parseKVConf(cfgKVCols, _kvParserList, getDataType());
		DataParserCfg.genCounters(context, _kvParserList);
		
		String mapKVCfgCols = _cfgMapKeyCols + DataParserCfg.SEPRATOR + _cfgMapValCols;
		ArrayList<String> colsName = DataParserCfg.parseColsName (mapKVCfgCols);
		DataParserCfg.genIndexsByName(colsName, _kvParserList);

	} // genKVParsers

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		
		readConfig (context);
		
		genKVParsers (context);		

	} // void setup

	@Override
	protected void reduce(Text in_key, Iterable<Text> in_values, Context context)
			throws IOException, InterruptedException {
		
		if (_bNoReduceMode) {
			for (Text in_value : in_values) {

				context.write(in_key, in_value);
			}
			return;
		}

		String inKey = in_key.toString().trim();
		for (Text value : in_values) {
			String redInput = inKey + ColParser.SEPRATOR + value.toString().trim();
			
			String[] kvCols = redInput.trim().split(ColParser.SEPRATOR_SPLIT);
			
			for (ColParser colParser : _kvParserList) {
				if (!colParser.inputVal(kvCols))
					_LOG.error(colParser.Dump());
			}

		} // for (Text value : values)

		
		String outRedKey = "";		
		for (int i = 0; i < _szKeyCols; ++i) {
			ColParser keyColParser = _kvParserList.get(i);
			outRedKey += keyColParser.toString() + ColParser.SEPRATOR;
			keyColParser.clear();
		}
		outRedKey = outRedKey.trim();
		

		String outRedVal = "";
		for (int i = _szKeyCols; i < _kvParserList.size(); ++i) {
			ColParser keyColParser = _kvParserList.get(i);
			outRedVal += keyColParser.toString() + ColParser.SEPRATOR;
			keyColParser.clear();
		}
		outRedVal = outRedVal.trim();
		
		// Output key and values
		if (outRedKey.isEmpty() || outRedVal.isEmpty()) {
			
			String err_msg = "key = " + in_key.toString() 
			+ ", value = " + in_values.toString()
			+ ", parse have empty as :" + " new key = " 
			+ outRedKey + ", new value = " + outRedVal
			+ ".\n Parse by Key Value parsers :" + DataParserCfg.dumpParser(_kvParserList);
			
			_LOG.error(err_msg);
			throw new IOException (err_msg);
			
		} // if (!strRedKey.toString().isEmpty() && !strRedVal.isEmpty())

		context.write(new Text(outRedKey), new Text(outRedVal));

	} // void reduce

}
