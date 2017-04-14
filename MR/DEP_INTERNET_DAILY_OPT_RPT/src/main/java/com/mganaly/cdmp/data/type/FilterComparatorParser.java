package com.mganaly.cdmp.data.type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * 
 * class FilterComparatorParser
 * 
 * SYNAX:
 * FILTER:ColName_StringCompareType_Value
 *
 */
public class FilterComparatorParser extends ColParser {
	
	private static final Log _LOG = LogFactory.getLog(FilterComparatorParser.class);

	String str_type_comp;
	String str_comparator;
	int int_filter_value = 0;
	String str_filter_value;


	public FilterComparatorParser(String filter_comparator) {
		super(filter_comparator.trim().split("_")[0]);
		
		String[] fc = filter_comparator.trim().split("_");
		if (fc.length != 4) {

			_LOG.error("FilterComparatorParser para is " + filter_comparator);
			return;
		}
		str_type_comp = fc[1];
		str_comparator = fc[2];
		if (0 == str_type_comp.compareToIgnoreCase("int")) {
			int_filter_value = Integer.parseInt(fc[3]);
		} else if (0 == str_type_comp.compareToIgnoreCase("string")) {
			str_filter_value = fc[3];
		}
		

		_cleanData 	= eDataCleanStrict.CUSTOME;
	}

	public eInvalidCounters getCounterId() {
		return eInvalidCounters.INV_FilterComparatorParser;
	}

	@Override
	protected boolean isDataClean(String strVal) {

		boolean bValide = false;
	
		if (0 == str_type_comp.compareToIgnoreCase("int")) {

			int value = Integer.parseInt(strVal);
			if (0 == str_comparator.compareToIgnoreCase("GTR")) {

				bValide = (value > int_filter_value);

			} else if (0 == str_comparator.compareToIgnoreCase("EQ")) {
				
				bValide = (value == int_filter_value);
				
			} else if (0 == str_comparator.compareToIgnoreCase("LESS")) {
				
				bValide = (value < int_filter_value);
				
			}else if (0 == str_comparator.compareToIgnoreCase("NEQ")) {

				bValide =  (value != int_filter_value);
				
			}
		} else if (0 == str_type_comp.compareToIgnoreCase("string")) {
			
			if (0 == str_comparator.compareToIgnoreCase("GTR")) {

				bValide = (str_filter_value.compareToIgnoreCase(strVal) < 0);

			} else if (0 == str_comparator.compareToIgnoreCase("EQ")) {

				bValide = (str_filter_value.compareToIgnoreCase(strVal) == 0);

			} else if (0 == str_comparator.compareToIgnoreCase("LESS")) {

				bValide = (str_filter_value.compareToIgnoreCase(strVal) > 0);

			} else if (0 == str_comparator.compareToIgnoreCase("NEQ")) {

				bValide = (str_filter_value.compareToIgnoreCase(strVal) != 0);
				
			}
		}

		return bValide;
	}

} // public static class FilterComparatorParser