package maxA.userProfile.impl.linearRegression;

import maxA.userProfile.IFeatureField;
import maxA.userProfile.feature.userUser.UserUserFeature;
import maxA.userProfile.impl.GenericFeatureVector;
import maxA.userProfile.impl.GenericFeatureVector_UserUser;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

/**
 * Created by max2 on 7/24/15.
 */
public class LR_FeatureVector_UserUser extends GenericFeatureVector_UserUser {

    public static final int LENGTH_LR_FeatureVector_UserUser = 3;

    // private double mTimeStamp;

    public LR_FeatureVector_UserUser() {

        super();
        // mTimeStamp = 0;
    }

    @Override
    protected int getLength() { return LENGTH_LR_FeatureVector_UserUser; }

    @Override
    protected GenericFeatureVector_UserUser createFeatureVector_UserUser() {

        return new LR_FeatureVector_UserUser();
    }

    /*
    public void setFieldValue(IFeatureField field, double value, double timeStamp) {

        // debugging starts here
        MaxLogger.debug(LR_FeatureVector_UserUser.class,
                        "---------------- [setFieldValue(X)] field :" + field.getName() + " , quantization:" + value);
        // debugging ends here

        mFieldValues[field.getIndex()] = value;
        mTimeStamp = timeStamp;
    }

    public LR_FeatureVector_UserUser combine(LR_FeatureVector_UserUser other) {

        if (other == null) {
            MaxLogger.error(LR_FeatureVector_UserUser.class,
                            ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, UserUserFeature.FEATURE_NAME));
        }

        LR_FeatureVector_UserUser result = new LR_FeatureVector_UserUser();

        double[] thisFieldValues = this.getFieldValues();
        double[] otherFieldValues = other.getFieldValues();

        int len = thisFieldValues.length;

        if (len != otherFieldValues.length) {
            MaxLogger.error(LR_FeatureVector_UserUser.class, ErrMsg.ERR_MSG_IncompatibleFeatureVector);
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
            MaxLogger.debug(LR_FeatureVector_UserUser.class,
                            "---------------- [combine] i:" + i +" thisVal:" + thisVal + " , otherVal:" + otherVal + "result:" + result.getFieldValues()[i]+ "----------------");
            // debugging ends here
        }

        return result;
    }
    */
}
