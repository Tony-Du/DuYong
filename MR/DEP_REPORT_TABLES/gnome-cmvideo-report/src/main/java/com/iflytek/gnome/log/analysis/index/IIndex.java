package com.iflytek.gnome.log.analysis.index;

import com.google.gson.JsonObject;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;

public interface IIndex
{
	public JsonObject getAdxIndexFields(AdAnalysisModel model);
}
