package com.mganaly.mapred;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;

public class join_base_mapper extends base_mapper {
	
	private static final Log _LOG = LogFactory.getLog(join_base_mapper.class);

	protected String _cfgLFlag = null;
	protected String _RcfgInCols;
	protected String _RcfgKeyCols;
	protected String _RcfgValCols;
	
	protected ArrayList<ColParser> _RInColsList		= new ArrayList<ColParser>();
	protected ArrayList<ColParser> _RKeyParserList 	= new ArrayList<ColParser>();
	protected ArrayList<ColParser> _RValParserList 	= new ArrayList<ColParser>();

	@Override
	protected void readConfig (Context context) throws IOException {
		super.readConfig(context);
		
		Configuration conf = context.getConfiguration();

		_RcfgInCols = conf.get(DataParserCfg.MAP_RIGHT_INPUT_COLS_CONF_NAME);
		if (null != _RcfgInCols) {
			_RcfgInCols = _RcfgInCols.trim();
		}

		_cfgLFlag = conf.get(DataParserCfg.TD_JOIN_LEFT_FLAG).trim();
		_RcfgKeyCols = conf.get(DataParserCfg.TD_JOIN_MAP_RIGHT_KEY_CONF).trim();
		_RcfgValCols = conf.get(DataParserCfg.TD_JOIN_MAP_RIGHT_VAL_CONF).trim();

	}
	

	protected void genKVParsers(Context context) throws IOException {
		
		super.genKVParsers(context);
				
		DataParserCfg.parseKVConf(_RcfgKeyCols, _RKeyParserList, getDataType());		
		DataParserCfg.genCounters(context, _RKeyParserList);
		

		DataParserCfg.parseKVConf(_RcfgValCols, _RValParserList, getDataType());		
		DataParserCfg.genCounters(context, _RValParserList);
		
		if (null != _RcfgInCols) {

			ArrayList<String> colsName = DataParserCfg.parseColsName (_RcfgInCols);			
			DataParserCfg.genIndexsByName(colsName, _RKeyParserList);
			DataParserCfg.genIndexsByName(colsName, _RValParserList);
		}

	} // genKVParsers
}
