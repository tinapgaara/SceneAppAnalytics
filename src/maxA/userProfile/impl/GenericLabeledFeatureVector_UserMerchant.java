package maxA.userProfile.impl;

import maxA.userProfile.IFeatureVector;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.feature.userMerchant.UserMerchantFeatureField;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;

/**
 * Created by TAN on 8/11/2015.
 */
public abstract class GenericLabeledFeatureVector_UserMerchant implements Serializable {

    protected GenericFeatureVector_UserMerchant mFeatureVector;

    protected double mLabel;
    protected double mTimeStamp;

    protected abstract GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant();

    protected GenericLabeledFeatureVector_UserMerchant() {

        mFeatureVector = null;

        mLabel = 0;
        mTimeStamp = 0 ;
    }

    public void setLabel(double label, double timestamp) {

        // debugging starts here
        MaxLogger.debug(GenericLabeledFeatureVector_UserMerchant.class,
                        "---------------- [setLabel(y)] label :" + label);
        // debugging ends here

        mLabel = label;
        mTimeStamp = timestamp;
    }

    public void addEnterMerchantTimestamp(double timestamp) {
        mFeatureVector.addEnterMerchantTimestamp(timestamp);
    }

    public void addLeaveMerchantTimestamp(double timestamp) {
        mFeatureVector.addLeaveMerchantTimestamp(timestamp);
    }

    public void addEnterDetailsTimestamp(double timestamp) {
        mFeatureVector.addEnterDetailsTimestamp(timestamp);
    }

    public void addLeaveDetailsTimestamp(double timestamp) {
        mFeatureVector.addLeaveDetailsTimestamp(timestamp);
    }

    public double getEnterMerchantTimeLength() {
        return mFeatureVector.getEnterMerchantTimeLength();
    }

    public double getEnterDetailsTimeLength() {
        return mFeatureVector.getEnterDetailsTimeLength();
    }

    public LabeledPoint getLabeledPoint() {

        if (mFeatureVector == null) {
            MaxLogger.error(this.getClass(), ErrMsg.ERR_MSG_NullFeatureVector);
            return null;
        }

        /*
        Vector v = mFeatureVector.getData();
        double[] arr = v.toArray();

        boolean emptyFlag = true;
        if (mLabel == 0) {
            for (int i = 0; i < arr.length; i++) {
                if (arr[i] != 0) {
                    emptyFlag = false;
                    break;
                }
            }
        }
        */

        LabeledPoint point = new LabeledPoint(mLabel, mFeatureVector.getData());

        return point;
    }

    public void setFieldValue(UserMerchantFeatureField field, double quantization) {

        // debugging starts here
        MaxLogger.debug(GenericLabeledFeatureVector_UserMerchant.class,
                        "---------------- [setFieldValue(X)] field :" + field.getName() + " , quantization:" + quantization);
        // debugging ends here

        mFeatureVector.setFieldValue(field, quantization);
    }

    public GenericLabeledFeatureVector_UserMerchant combine(GenericLabeledFeatureVector_UserMerchant other) {

        if (other == null) {
            MaxLogger.error(this.getClass(),
                    ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, UserMerchantFeature.FEATURE_NAME));
        }

        GenericLabeledFeatureVector_UserMerchant result = createLabeledFeatureVector_UserMerchant();
        result.mFeatureVector = mFeatureVector.combine(other.mFeatureVector);

        if (this.mLabel == other.mLabel) {
            result.mLabel = this.mLabel;
            result.mTimeStamp = (this.mTimeStamp > other.mTimeStamp) ? this.mTimeStamp : other.mTimeStamp;
        }
        else if (this.mLabel == 0) {
            result.mLabel = other.mLabel;
            result.mTimeStamp = other.mTimeStamp;
        }
        else if (other.mLabel == 0) {
            result.mLabel = this.mLabel;
            result.mTimeStamp = this.mTimeStamp;
        }
        else {
            if (this.mTimeStamp >= other.mTimeStamp) {
                result.mLabel = this.mLabel;
                result.mTimeStamp = this.mTimeStamp;
            }
            else {
                result.mLabel = other.mLabel;
                result.mTimeStamp = other.mTimeStamp;
            }
        }
        return result;
    }

    public IFeatureVector getVector() {
        return mFeatureVector;
    }

}
