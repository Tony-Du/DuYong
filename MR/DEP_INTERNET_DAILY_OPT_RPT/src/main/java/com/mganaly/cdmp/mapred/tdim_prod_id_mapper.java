package com.mganaly.cdmp.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mganaly.cdmp.data.tbl.cdmp_base_tbl;
import com.mganaly.cdmp.data.tbl.tdim_prod_id;

/**
 * 
 * @author xxx
 * 
 * class cdmp_tdim_prod_id_mapper
 *
 */
public class tdim_prod_id_mapper extends cdmp_base_mapper{
	
	private static final Log _LOG = LogFactory.getLog(tdim_prod_id_mapper.class);
	
	
	@Override
	protected cdmp_base_tbl getDataType() {
		if (null == _dataType) {
			_dataType = new tdim_prod_id();
		}
		return _dataType;
	}

}
