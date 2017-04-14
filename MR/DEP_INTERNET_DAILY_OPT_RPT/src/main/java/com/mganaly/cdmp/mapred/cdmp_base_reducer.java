package com.mganaly.cdmp.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import com.mganaly.mapred.base_reducer;

public class cdmp_base_reducer extends base_reducer {


	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		super.reduce(key, values, context);

	} // void reduce
	
}
