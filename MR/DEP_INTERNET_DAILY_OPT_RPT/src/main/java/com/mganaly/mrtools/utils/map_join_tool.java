package com.mganaly.mrtools.utils;

import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mganaly.conf.GlobalEv;
import com.mganaly.mapred.map_join_mapper;

public class map_join_tool extends join_base_tool {
	
	private static final Log _LOG = LogFactory.getLog(map_join_tool.class);

	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		int code = MR_SUCCESS;
		
		// Configuration
		setCommConf();
		
		confCommJobParas();	

		String inCacheFiles = _mapParams.get(PARAM_INPATH);
		

		if (null != _inStartDate) {
			inCacheFiles +=  "/" + _inStartDate;
		}
		
		cacheFiles(inCacheFiles);
		
		_mr_job.setMapperClass(map_join_mapper.class);
		//_mr_job.setReducerClass(cdmp_td_mapjoin_reducer.class);
		
		setDefaultPath(_mr_job);

		return code;
	}	//run(String[] args) 
	
	
	private boolean cacheFiles (String dirPath) throws IOException {
		
		FileSystem fs = FileSystem.get(_conf);
		FileStatus[] filestatus = fs.listStatus(new Path(dirPath));
		_LOG.info("cacheFiles : " + dirPath);
		for (FileStatus file : filestatus) {
			
			if (file.isDirectory()) {
				_LOG.info(file.getPath().toString() + " is directory. recursion cache file...");
				cacheFiles (file.getPath().toString());
				continue;
			}
			
			URI uri_inpath = new Path(file.getPath().toString()).toUri();
			_LOG.info("left_path(Cached) : " + uri_inpath);
			DistributedCache.addCacheFile(uri_inpath, _mr_job.getConfiguration());
		}
		
		return true;
	}
	

	protected void setDefaultPath(Job paraJob) throws IOException {

		String rightPath = _mapParams.get(PARAM_INPATH_R);
		
		
		if (null != _inRStartDate) {
			rightPath +=  "/" + _inRStartDate;
		}
		
		addInputPath (paraJob, rightPath);
		
		String outPath = _mapParams.get(PARAM_OUTPATH);
		
		if (null != _outStartDate) outPath += "/" + _outStartDate;
		_LOG.info("outputPath : " + outPath);
		GlobalEv.checkAndRemove(_conf, outPath);
		FileOutputFormat.setOutputPath(paraJob, new Path(outPath));

	} // setDefaultPath

}
