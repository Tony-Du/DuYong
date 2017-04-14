package com.mganaly.cdmp.data.type;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * 
 * @author xxx
 * 
 * class SumCntParser
 * 
 *
 */
public class SumCntParser extends ColParser {
	private HashMap<String, Integer> map_cnt = new HashMap<String, Integer>();
	int index = 0;

	public SumCntParser(String str_idx) {
		super(str_idx);
		
		index = Integer.parseInt(str_idx);
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_NULL;
	}

	public String toString() {

		long sum_cnt = 0;
		Iterator iter = map_cnt.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String cmdkey = (String) entry.getKey();

			Integer cnt = map_cnt.get(cmdkey);
			if (null != cnt)
				sum_cnt += cnt;
		} // while (iter.hasNext())

		return String.valueOf(sum_cnt);
	}

	public boolean inputVal(String strVal) {		
		boolean bSucceed = super.inputVal(strVal);
		
		if (bSucceed) {
			Integer size = map_cnt.get(strVal);
			if (null != size) {
				map_cnt.put(strVal, ++size);
			} else {
				map_cnt.put(strVal, 1);
			}
		} else {
			_counter.increment(1);
		}

		return true;
	}

	public void clear() {
		map_cnt.clear();
	}

	@Override
	protected boolean isDataClean(String data) {
		// TODO Auto-generated method stub
		return true;
	}
} // public static class SumParser
