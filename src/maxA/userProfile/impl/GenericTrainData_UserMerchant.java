package maxA.userProfile.impl;

import maxA.userProfile.ITrainData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Created by TAN on 8/13/2015.
 */
public class GenericTrainData_UserMerchant implements ITrainData {

    protected JavaRDD<LabeledPoint> mTraindata;

    protected GenericTrainData_UserMerchant(JavaRDD<LabeledPoint> data) {
        mTraindata = data;
    }

    public JavaRDD<LabeledPoint> getData() {
        return mTraindata;
    }

    @Override
    public void release() {
        mTraindata = null;
    }

}
