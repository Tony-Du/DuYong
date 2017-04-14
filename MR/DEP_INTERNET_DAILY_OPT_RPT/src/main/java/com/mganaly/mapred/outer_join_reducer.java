package com.mganaly.mapred;

public class outer_join_reducer extends join_base_reducer {

	@Override
	protected boolean dataCollectOK() {
		
		return ((_LTBLValCnt > 0)||(_RTBLValCnt > 0));
		
	}

}
