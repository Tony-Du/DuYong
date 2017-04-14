package com.mganaly.cdmp.data.type;

/**
 * 
 * class KeyValPair 
 */
public class KeyValPair 
{
	public String _key = null;
	public String _val = null;
	
	public KeyValPair () {
		_key = "";
		_val = "";
	}
	
	public KeyValPair (String key, String val) {
		_key = key;
		_val = val;
	}
	
	public boolean isValid() {
		return ((null!=_key)&&(!_key.isEmpty()) 
				&& (null!=_val)&&(!_val.isEmpty()));
	}
}

