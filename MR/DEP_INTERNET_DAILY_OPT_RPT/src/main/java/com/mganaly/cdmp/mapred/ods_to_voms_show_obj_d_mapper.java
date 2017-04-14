package com.mganaly.cdmp.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.tbl.ods_to_voms_show_obj_d;

public class ods_to_voms_show_obj_d_mapper extends cdmp_base_mapper {

	private static final Log _LOG = LogFactory.getLog(ods_to_voms_show_obj_d_mapper.class);
	

	@Override
	protected cdmp_base_tbl getDataType() {
		if (null == _dataType) {
			_dataType = new ods_to_voms_show_obj_d();
		}
		return _dataType;
	}

}
