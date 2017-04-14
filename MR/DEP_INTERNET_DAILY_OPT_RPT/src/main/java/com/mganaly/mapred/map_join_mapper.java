package com.mganaly.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.cdmp.data.type.KeyValPair;


public class map_join_mapper extends join_base_mapper {
	
	private static final Log _LOG = LogFactory.getLog(map_join_mapper.class);

	protected HashMap<String, String> left_table = new HashMap<String, String>();
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);

		String line = "";
		URI[] uris = context.getCacheFiles();
		

		for (URI uri : uris) {
			{
				FileSystem fs = FileSystem.get(uri, context.getConfiguration());
				FSDataInputStream in = fs.open(new Path(uri.getPath()));
				BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
				while (null != (line = br.readLine())) {
					KeyValPair kv = DataParserCfg.processKVData(line,
							                 _keyParserList, _valParserList);
					
					if (null != kv && kv.isValid()) {
						left_table.put(kv._key, kv._val);
					}
				}
				br.close();
			} // if 
		} // for (Path p
	} // void setup

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();

		String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();

		if (!pathName.contains(_cfgLFlag)) {

			KeyValPair kv = DataParserCfg.processKVData(line, 
					_RKeyParserList, _RValParserList);

			if (null != kv && kv.isValid()) {

				String mapVal = left_table.get(kv._key);
				if ( null!= mapVal && !mapVal.isEmpty()) {
					mapVal += ColParser.SEPRATOR + kv._val;
					context.write(new Text(kv._key), new Text(mapVal));
				}
			}
			else {
				String err_info = "error line parsed : "
						 + DataParserCfg.dumpParser(_RKeyParserList)
						 + DataParserCfg.dumpParser(_RValParserList);
				
				_LOG.info(err_info);
				
				throw new IOException(err_info);
			}
		}
	} // protected void map

}
