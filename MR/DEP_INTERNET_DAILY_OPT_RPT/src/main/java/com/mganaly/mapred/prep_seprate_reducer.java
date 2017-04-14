package com.mganaly.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.DataParserCfg;

public class prep_seprate_reducer extends base_reducer {
	
	private static final Log _LOG = LogFactory.getLog(prep_seprate_reducer.class);
	private String _seprator = null;
	
	@Override
	protected void readConfig (Context context) {
		super.readConfig(context);
		
		Configuration conf = context.getConfiguration();		
		_seprator			= conf.get(DataParserCfg.SEPRATOR_TARGET);
	}
	

	private String transSep(String strCols) throws IOException {

		String outStr = "";
		String[] cols = strCols.split(ColParser.SEPRATOR_SPLIT);
		
		for (String col : cols) {
			outStr += col + _seprator;
		}

		outStr = outStr.substring(0, outStr.length() - 1);


		return outStr;
	}
	
	@Override
	protected void reduce(Text in_key, Iterable<Text> in_values, Context context)
			throws IOException, InterruptedException {
		
		String inKey = in_key.toString().trim();
		
		String strKey = transSep (inKey);
		for (Text value : in_values) {
			
			String inVal = value.toString().trim();
			String strVal = transSep (inVal);
			context.write(new Text(strKey), new Text(strVal));
		} // for (Text value : values)

	} // void reduce

}
