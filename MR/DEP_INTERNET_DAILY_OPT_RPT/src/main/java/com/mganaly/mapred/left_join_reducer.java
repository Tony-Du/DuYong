package com.mganaly.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class left_join_reducer  extends join_base_reducer {
	
	private static final Log _LOG = LogFactory.getLog(left_join_reducer.class);
	

	@Override
	protected boolean dataCollectOK () {
		
		return (_LTBLValCnt > 0);
	}

}