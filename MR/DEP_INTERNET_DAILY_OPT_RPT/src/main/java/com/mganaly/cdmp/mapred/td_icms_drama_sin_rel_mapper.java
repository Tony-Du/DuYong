package com.mganaly.cdmp.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.tbl.td_icms_drama_sin_rel;

public class td_icms_drama_sin_rel_mapper extends cdmp_base_mapper {
	
	private static final Log _LOG = LogFactory.getLog(tdim_prod_id_mapper.class);
	
	
	@Override
	protected cdmp_base_tbl getDataType() {
		if (null == _dataType) {
			_dataType = new td_icms_drama_sin_rel();
		}
		return _dataType;
	}


}
