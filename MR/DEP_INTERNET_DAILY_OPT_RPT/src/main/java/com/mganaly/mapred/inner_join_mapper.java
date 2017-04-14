package com.mganaly.mapred;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.KeyValPair;

public class inner_join_mapper extends join_base_mapper {

	private static final Log _LOG = LogFactory.getLog(inner_join_mapper.class);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().trim();
		String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();

		if (pathName.contains(_cfgLFlag)) {
			
			KeyValPair kv = DataParserCfg.processKVData(line, _keyParserList, 
					_valParserList);


			if (null !=kv && kv.isValid()) {
				context.write(new Text(kv._key), new Text(kv._val+ ColParser.SEPRATOR + DataParserCfg.JOIN_L ));
			}

		} else {
			KeyValPair kv = DataParserCfg.processKVData(line, _RKeyParserList, 
					_RValParserList);
			
			if (null !=kv && kv.isValid()) {
				context.write(new Text(kv._key), new Text(kv._val + ColParser.SEPRATOR + DataParserCfg.JOIN_R));
			}
		}

		//context.write(new Text(_joinKey), _combineValues);

	} // void map
}
