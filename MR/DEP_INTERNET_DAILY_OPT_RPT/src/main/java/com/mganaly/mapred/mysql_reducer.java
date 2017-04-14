package com.mganaly.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.cdmp.data.type.mysql_tbl_writer;

public class mysql_reducer extends Reducer<Text,Text,mysql_tbl_writer,mysql_tbl_writer> 
{  
    public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException 
    {  
    	for (Text val : values) {
    		String [] cols = val.toString().trim().split(ColParser.SEPRATOR);
    		context.write(new mysql_tbl_writer(cols),null); 
    	}
    }  
}
