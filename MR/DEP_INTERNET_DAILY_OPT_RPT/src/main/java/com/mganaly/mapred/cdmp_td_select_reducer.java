package com.mganaly.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;

public class cdmp_td_select_reducer extends base_reducer {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {

		super.reduce(key, values, context);
		
	} //void reduce
	
	@Override
	protected cdmp_base_tbl getDataType() {
		if (null == _data_type) {
			_data_type = new cdmp_base_tbl();
		}
		return _data_type;
	}

}
