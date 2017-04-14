package com.mganaly.cdmp.data.type;

import java.text.ParseException;

import com.mganaly.conf.DateHelper;

public class AvaDaylyParser  extends SumParser{
	
	private int _days = 0; 

	public AvaDaylyParser(String colDef) {
		super(colDef);		
	}

	@Override
	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_AvaDaylyParser;
	}
	
	protected void calcDays() {
		String fromDay = _conf.get(DataParserCfg.MR_FROM_DAY);
		String defaultDay = _conf.get(DataParserCfg.MR_START_DAY);
		try {		
			_days =  DateHelper.daysBetween(fromDay, defaultDay) + 1;
			
			if (_days <= 0) {
					throw new ParseException (fromDay + " to " + defaultDay + " is " + _days, getIndex());
			}
		} catch (ParseException e) {
			_days = 1;
			e.printStackTrace();
		}
	}
	
	public String toString() {
		if (0 == _days) {
			calcDays();
		}
		return Long.toString(_cnt_val/_days);
	}

}
