package maxA.userProfile.impl;

import maxA.userProfile.IFeatureField;
import maxA.userProfile.feature.userUser.UserUserFeature;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

/**
 * Created by max2 on 8/13/15.
 */
public abstract class GenericFeatureVector_UserUser extends GenericFeatureVector {

    protected abstract GenericFeatureVector_UserUser createFeatureVector_UserUser();

    private double mTimeStamp;

    public void setFieldValue(IFeatureField field, double value, double timeStamp) {

        // debugging starts here
        MaxLogger.debug(GenericFeatureVector_UserUser.class,
                "---------------- [setFieldValue(X)] field :" + field.getName() + " , quantization:" + value);
        // debugging ends here

        mFieldValues[field.getIndex()] = value;
        mTimeStamp = timeStamp;
    }

    public GenericFeatureVector_UserUser combine(GenericFeatureVector_UserUser other) {

        if (other == null) {
            MaxLogger.error(GenericFeatureVector_UserUser.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, UserUserFeature.FEATURE_NAME));
        }

        GenericFeatureVector_UserUser result = createFeatureVector_UserUser();

        double[] thisFieldValues = this.getFieldValues();
        double[] otherFieldValues = other.getFieldValues();

        int len = thisFieldValues.length;

        if (len != otherFieldValues.length) {
            MaxLogger.error(GenericFeatureVector_UserUser.class, ErrMsg.ERR_MSG_IncompatibleFeatureVector);
        }

        for (int i=0 ; i < len ; i++) {

            double otherVal = otherFieldValues[i];
            double thisVal = thisFieldValues[i];

            if (otherVal == 0) {
                result.getFieldValues()[i] = thisVal;
                result.mTimeStamp = this.mTimeStamp;
            }
            else if (thisVal == 0) {
                result.getFieldValues()[i] = otherVal;
                result.mTimeStamp = other.mTimeStamp;
            }
            else {
                if (this.mTimeStamp >= other.mTimeStamp) {
                    result.getFieldValues()[i] = thisVal;
                    result.mTimeStamp = this.mTimeStamp;
                }
                else {
                    result.getFieldValues()[i] = otherVal;
                    result.mTimeStamp = other.mTimeStamp;
                }
            }
            // debugging starts here
            MaxLogger.debug(GenericFeatureVector_UserUser.class,
                    "---------------- [combine] i:" + i +" thisVal:" + thisVal + " , otherVal:" + otherVal + "result:" + result.getFieldValues()[i]+ "----------------");
            // debugging ends here
        }

        return result;
    }
}
