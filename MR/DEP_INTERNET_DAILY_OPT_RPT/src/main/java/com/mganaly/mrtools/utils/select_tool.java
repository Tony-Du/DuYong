package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.cdmp.data.type.DataParserCfg;
import com.mganaly.mapred.cdmp_td_select_mapper;
import com.mganaly.mapred.cdmp_td_select_reducer;

public class select_tool  extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(select_tool.class);
	
	private int _data_len		= 0;

	public void setDataLen (int dataLen) {
		_data_len = dataLen;
	}
	

	@Override
	protected int confJobParas(String[] args) throws IOException {
		_LOG.info("confJobParas");
		
		if (0 >= _data_len) {
			throw new IOException("preprocess_data_len is " + _data_len 
					+ ", Please call and check setDataLen parameter");
		}
		
		int code = MR_SUCCESS;
		
		setCommConf();
		
		confCommJobParas();

		_mr_job.setMapperClass(cdmp_td_select_mapper.class);
		_mr_job.setReducerClass(cdmp_td_select_reducer.class);
		
		setDefaultPath(_mr_job);

		return code;
	} ////run(String[] args)
	

	@Override
	protected void setCommConf() throws IOException {
		
		super.setCommConf();
		
		_conf.set(DataParserCfg.PREPROCESS_DATA_LENGTH, Integer.toString(_data_len));
		_LOG.info(DataParserCfg.PREPROCESS_DATA_LENGTH + " : " + _data_len);
	}

}
