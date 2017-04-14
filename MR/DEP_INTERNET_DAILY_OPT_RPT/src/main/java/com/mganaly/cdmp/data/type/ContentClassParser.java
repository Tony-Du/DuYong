package com.mganaly.cdmp.data.type;

/**
 * 
 * class ContentClassParser
 *
 */
public class ContentClassParser extends ColParser {
	
	private enum eContentClass {
		MOVIE//'电影',
		, TV// '电视剧',
		, VARIETY //variety'综艺',
		, CARTOON//'动漫'
	}
	
	private final String SEPRATOR_SPLIT = "\\x7c"; // | 
	private String [] _contClass;
	
	public ContentClassParser(String appName) {
		super(appName);
		translContClass(appName);
	}


	protected boolean isDataClean(String strVal) {
		boolean isVal = false;
		for (String contentClass : _contClass) {
			if (strVal.contains(contentClass)) {
				isVal = true;
				break;
			}
		}
		return isVal;
	}
	
	

	private void translContClass (String inColDefVal) {
		
		String colDefVal = inColDefVal.substring(_colName.length()).trim();
		
		String [] inContClasses = colDefVal.trim().split(SEPRATOR_SPLIT);
		
		_contClass = new String[inContClasses.length];
		
		for (int i = 0; i < inContClasses.length; ++i) {
			
			String inContClass = inContClasses[i].trim();
			
			if (0 == inContClass.compareToIgnoreCase(eContentClass.MOVIE.name())) {
				_contClass[i] = "电影";
				
			}
			else if (0 == inContClass.compareToIgnoreCase(eContentClass.TV.name())) {
				_contClass[i] = "电视剧";
				
			}
			
			else if (0 == inContClass.compareToIgnoreCase(eContentClass.VARIETY.name())) {
				_contClass[i] = "综艺";
				
			}
			else if (0 == inContClass.compareToIgnoreCase(eContentClass.CARTOON.name())) {
				_contClass[i] = "动漫";
				
			}
			else {
				_contClass[i] = "ERROR";
			}
		}
	}


	@Override
	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_CON_CLASS;
	}

} //class ContentClassParser
