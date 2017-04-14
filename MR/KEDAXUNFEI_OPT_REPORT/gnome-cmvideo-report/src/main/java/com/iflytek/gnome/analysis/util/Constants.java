package com.iflytek.gnome.analysis.util;

import com.iflytek.gnome.analysis.mapreduce.MRCheat.AdxPVModel;
import com.iflytek.gnome.analysis.mobile.dsp.DspModelV2;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.model.DmModel;
import com.iflytek.gnome.analysis.model.DspModel;
import com.iflytek.gnome.analysis.model.GnomeUserInfo;

public class Constants {

    public static final String MODEL_OUTPUT_BASE = "online/vc_log/extract/";

    public static final String SUNFLOWER_USER_DIR = "/user/sunflower";

    // adx有关模型
    // online/vc_log/extract/bj/AdAnalysisModel
    public static final String ADMODEL_BJ_DIR = Constants.MODEL_OUTPUT_BASE + "bj/"
            + AdAnalysisModel.class.getSimpleName();

    // online/vc_log/extract/AdAnalysisModel
    public static final String ADMODEL_DIR = Constants.MODEL_OUTPUT_BASE + AdAnalysisModel.class.getSimpleName();

    // online/vc_log/extract/bj/AdxPVModel
    public static final String ADXPVMMODEL_DIR = Constants.MODEL_OUTPUT_BASE + "bj/" + AdxPVModel.class.getSimpleName();

    // 未知模型
    public static final String DMMODEL_DIR = Constants.MODEL_OUTPUT_BASE + DmModel.class.getSimpleName();

    // dsp1.0有关模型
    public static final String DSPMODEL_BJ_DIR = Constants.MODEL_OUTPUT_BASE + "bj/" + DspModel.class.getSimpleName();

    public static final String DSPMODEL_DIR = Constants.MODEL_OUTPUT_BASE + DspModel.class.getSimpleName();
    // public static final String DSPPVMODEL_DIR = Constants.MODEL_OUTPUT_BASE +
    // "bj/" + DspPVModel.class.getSimpleName();

    // 未知用户数据集
    public static final String GNOMEUSER_DIR = "online/vc_log/User/" + GnomeUserInfo.class.getSimpleName();

    // dsp2.0有关模型
    // online/vc_log/extract/bj/DspModelV2
    public static final String DSPMODELV2_BJ_DIR = Constants.MODEL_OUTPUT_BASE + "bj/"
            + DspModelV2.class.getSimpleName();

    // online/vc_log/extract/DspModelV2
    public static final String DSPMODELV2_DIR = Constants.MODEL_OUTPUT_BASE + DspModelV2.class.getSimpleName();

    // dmp标签用户标签数据集
    // online/vc_log/extract/bj/DmpTagModel
    public static final String DMP_USER_TAG_DIR = Constants.MODEL_OUTPUT_BASE + "bj/" + "DmpTagModel";

    // 经过dmpmerge后的AdAnalysisModel
    public static final String ADMODEL_AFTER_DMP_MERGE_DIR = Constants.MODEL_OUTPUT_BASE + "dmp/"
            + AdAnalysisModel.class.getSimpleName();
}
