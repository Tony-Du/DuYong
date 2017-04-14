package com.mganaly.cdmp.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.tbl.td_pub_visit_log_d;

public class td_pub_visit_log_d_mapper extends cdmp_base_mapper {

	private static final Log _LOG = LogFactory.getLog(td_pub_visit_log_d_mapper.class);
	
	@Override
	protected cdmp_base_tbl getDataType() {
		if (null == _dataType) {
			_dataType = new td_pub_visit_log_d();
		}
		return _dataType;
	}

}
