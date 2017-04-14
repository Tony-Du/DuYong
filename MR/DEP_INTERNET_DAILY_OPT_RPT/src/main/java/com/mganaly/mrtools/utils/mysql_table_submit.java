package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.mganaly.cdmp.data.type.mysql_tbl_writer;
import com.mganaly.conf.GlobalEv;
import com.mganaly.mapred.mysql_mapper;
import com.mganaly.mapred.mysql_reducer;

/**
 * 
 * class mysql_table_submit
 * 
 *
 */
public class mysql_table_submit extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(mysql_table_submit.class);
	
 
    public int run(String[] args) throws Exception 
    {
    
    	parseParams(args);
    	
    	setCommConf();
		
        DistributedCache.addFileToClassPath(new Path(GlobalEv.mysqlCntor), _conf);

        DBConfiguration.configureDB(_conf
        		, GlobalEv.mysqlJDBC
        		, GlobalEv.mysqlDB
        		, GlobalEv.mysqlUsr
        		, GlobalEv.mysqlPwd
        		);
    	
    	
        Job job = new Job(_conf,mysql_table_submit.class.getSimpleName()+getCurTime());  
        job.setJarByClass(mysql_table_submit.class);  
          
        job.setMapperClass(mysql_mapper.class);  
        job.setReducerClass(mysql_reducer.class);  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(mysql_tbl_writer.class);  
        job.setOutputValueClass(mysql_tbl_writer.class); 
        
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(DBOutputFormat.class);
        job.setNumReduceTasks(GlobalEv.REDUCE_NUM_ONE);
        
        int COL_NUM = Integer.parseInt(_mapParams.get(PARAM_TABLE_COLS_NUM));
        DBOutputFormat.setOutput(job, _mapParams.get(PARAM_TABLE_NAME), COL_NUM);
        
        setDefaultPath(job);
        
        
        int code = (job.waitForCompletion(true) ? 0 : 1);
        return code;
    }


	@Override
	protected int confJobParas(String[] args) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
