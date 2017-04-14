package com.mganaly.mrtools.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.tbl.td_userid_usernum;
import com.mganaly.cdmp.mapred.cdmp_base_mapper;
import com.mganaly.cdmp.mapred.cdmp_base_reducer;

public class td_userid_usernum_tool extends MRCmdTool {
	
	private static final Log _LOG = LogFactory.getLog(td_userid_usernum_tool.class);
	
	/**
	 * 
	 * class td_cms_content_attr_d_mapper
	 *
	 */
	public static class td_userid_usernum_mapper extends cdmp_base_mapper {
		
		
		@Override
		protected cdmp_base_tbl getDataType() {
			if (null == _dataType) {
				_dataType = new td_userid_usernum();
			}
			return _dataType;
		}

	} //class td_cms_content_attr_d_mapper
	
	
	
	protected int confJobParas (String[] args) throws IOException {
		int code = MR_SUCCESS;

		setCommConf();
		
		confCommJobParas();
		
		_mr_job.setMapperClass(td_userid_usernum_mapper.class);
		_mr_job.setReducerClass(cdmp_base_reducer.class);
		
		setDefaultPath(_mr_job);
		
		return code;
	}

}
