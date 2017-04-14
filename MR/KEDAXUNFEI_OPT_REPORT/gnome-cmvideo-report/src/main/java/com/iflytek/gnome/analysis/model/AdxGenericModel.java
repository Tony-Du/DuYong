package com.iflytek.gnome.analysis.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.iflytek.generic.model.AvroObjectMap;

public abstract class AdxGenericModel {
	
	/**
	 * 非基本字段
	 */
	public AvroObjectMap nonBasicFields = new AvroObjectMap(new HashMap<CharSequence, Object>());
	
	public final static String MODEL_NAME = "dc.model.name"; 
	public final static String MODEL_RECIEVE_TS = "dc.model.recieveTs";
	
	abstract public String getModelName();
	
	public void put(String key, Object value)
	{
		nonBasicFields.getData().put(key, value);
	}
	
	public void remove(String key){
		nonBasicFields.getData().remove(key);
	}

	public String getString(String fieldName)
	{
		return (String) nonBasicFields.getData().get(fieldName);
	}

	public Integer getInteger(String fieldName)
	{
		return (Integer) nonBasicFields.getData().get(fieldName);
	}

	public Integer getInteger(String fieldName, int defaultValue)
	{
		Integer ret = getInteger(fieldName);
		if (null == ret)
			return defaultValue;
		return ret;
	}

	public Long getLong(String field)
	{
		return (Long) nonBasicFields.getData().get(field);
	}

	public Float getFloat(String field)
	{
		return (Float) nonBasicFields.getData().get(field);
	}

	public Double getDouble(String field)
	{
		return (Double) nonBasicFields.getData().get(field);
	}

	public Boolean getBoolean(String field)
	{
		return (Boolean) nonBasicFields.getData().get(field);
	}

	public boolean getBoolean(String field, boolean defaultValue)
	{
		Boolean ret = getBoolean(field);
		if (null == ret)
			return defaultValue;
		return ret;
	}

	public Byte[] getBytes(String field)
	{
		return (Byte[]) nonBasicFields.getData().get(field);
	}

	@SuppressWarnings("rawtypes")
	public Map getMap(String field)
	{
		return (Map) nonBasicFields.getData().get(field);
	}

	@SuppressWarnings("rawtypes")
	public List getList(String field)
	{
		return (List) nonBasicFields.getData().get(field);
	}

}

