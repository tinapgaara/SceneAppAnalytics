package maxA.userProfile.impl;

import maxA.common.Constants;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.impl.linearRegression.LR_FeatureVector_UserUser;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by TAN on 8/11/2015.
 */
public abstract class GenericFeatureVector_UserMerchant extends GenericFeatureVector {

    protected List<Double> mEnterMerchantTimestamps;
    protected List<Double> mLeaveMerchantTimestamps;

    protected List<Double> mEnterDetailsTimestamps;
    protected List<Double> mLeaveDetailsTimestamps;

    protected abstract GenericFeatureVector_UserMerchant createFeatureVector_UserMerchant();

    protected GenericFeatureVector_UserMerchant() {

        super();

        mEnterMerchantTimestamps = null;
        mLeaveMerchantTimestamps = null;

        mEnterDetailsTimestamps = null;
        mLeaveDetailsTimestamps = null;
    }
    public void addEnterMerchantTimestamp(double timestamp) {

        if (mEnterMerchantTimestamps == null) {
            mEnterMerchantTimestamps = new ArrayList<Double>();
        }
        mEnterMerchantTimestamps.add(new Double(timestamp));
    }

    public void addLeaveMerchantTimestamp(double timestamp) {

        if (mLeaveMerchantTimestamps == null) {
            mLeaveMerchantTimestamps = new ArrayList<Double>();
        }
        mLeaveMerchantTimestamps.add(new Double(timestamp));
    }

    public void addEnterDetailsTimestamp(double timestamp) {

        if (mEnterDetailsTimestamps == null) {
            mEnterDetailsTimestamps = new ArrayList<Double>();
        }
        mEnterDetailsTimestamps.add(new Double(timestamp));
    }

    public void addLeaveDetailsTimestamp(double timestamp) {

        if (mLeaveDetailsTimestamps == null) {
            mLeaveDetailsTimestamps = new ArrayList<Double>();
        }
        mLeaveDetailsTimestamps.add(new Double(timestamp));
    }

    public double getEnterMerchantTimeLength() {

        return getTimeLengthSum(mEnterMerchantTimestamps, mLeaveMerchantTimestamps);
    }

    public double getEnterDetailsTimeLength() {

        return getTimeLengthSum(mEnterDetailsTimestamps, mLeaveDetailsTimestamps);
    }

    private double getTimeLengthSum(List<Double> enterTimestamps, List<Double> leaveTimestamps) {

        if ( (enterTimestamps == null) || (enterTimestamps.isEmpty()) ) {
            return 0;
        }

        if ( (leaveTimestamps == null) || (leaveTimestamps.isEmpty()) ) {
            return Constants.THRESHOLD_EnterMerchantTimeLength_InSeconds + 1; // no leave
            // any value greater than THRESHOLD_EnterMerchantTimeLength_InSeconds will be quantized into 1
        }

        Collections.sort(enterTimestamps);
        Collections.sort(leaveTimestamps);

        int lengthOfEnterTimestamps = enterTimestamps.size();
        int lengthOfLeaveTimestamps = leaveTimestamps.size();

        double timeLengthSum = 0;

        double enterTimestamp, leaveTimestamp;
        int i = 0, j = 0;
        boolean matched;
        while (i < lengthOfEnterTimestamps) {
            enterTimestamp = enterTimestamps.get(i);
            i++;

            matched = false;
            while (j < lengthOfLeaveTimestamps) {
                leaveTimestamp = leaveTimestamps.get(j);
                j++;

                if (leaveTimestamp >= enterTimestamp) {
                    matched = true;
                    timeLengthSum += (leaveTimestamp - enterTimestamp);
                    break;
                }
            }

            if ( ! matched ) {
                timeLengthSum = Constants.THRESHOLD_EnterMerchantTimeLength_InSeconds + 1; // no matched leave
                // any value greater than THRESHOLD_EnterMerchantTimeLength_InSeconds will be quantized into 1

                break;
            }
        }

        return timeLengthSum;
    }

    public GenericFeatureVector_UserMerchant combine(GenericFeatureVector_UserMerchant other) {

        if (other == null) {
            MaxLogger.error(GenericFeatureVector_UserMerchant.class,
                            ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, UserMerchantFeature.FEATURE_NAME));
        }

        GenericFeatureVector_UserMerchant result = this.copy();
        result.combineFieldValues(other);

        result.mEnterMerchantTimestamps = combineLists(
                result.mEnterMerchantTimestamps, other.mEnterMerchantTimestamps);
        result.mLeaveMerchantTimestamps = combineLists(
                result.mLeaveMerchantTimestamps, other.mLeaveMerchantTimestamps);

        result.mEnterDetailsTimestamps = combineLists(
                result.mEnterDetailsTimestamps, other.mEnterDetailsTimestamps);
        result.mLeaveDetailsTimestamps = combineLists(
                result.mLeaveDetailsTimestamps, other.mLeaveDetailsTimestamps);

        return result;
    }

    protected GenericFeatureVector_UserMerchant copy() {

        GenericFeatureVector_UserMerchant theCopy = createFeatureVector_UserMerchant();

        theCopy.mFieldValues = copyFieldValues();

        theCopy.mEnterMerchantTimestamps = copyList(mEnterMerchantTimestamps);
        theCopy.mLeaveMerchantTimestamps = copyList(mLeaveMerchantTimestamps);

        theCopy.mEnterDetailsTimestamps = copyList(mEnterDetailsTimestamps);
        theCopy.mLeaveDetailsTimestamps = copyList(mLeaveDetailsTimestamps);

        return theCopy;
    }

    private List<Double> copyList(List<Double> list) {

        if (list == null) {
            return null;
        }

        List<Double> newList = new ArrayList<Double>();
        newList.addAll(list);

        return newList;
    }

    private List<Double> combineLists(List<Double> list_1, List<Double> list_2) {

        if (list_2 == null) {
            return list_1;
        }

        if (list_1 == null) {
            return copyList(list_2);
        }

        list_1.addAll(list_2);

        return list_1;
    }


    @Override
    public void release() {

        super.release();

        if (mEnterMerchantTimestamps != null) {
            mEnterMerchantTimestamps.clear();
            mEnterMerchantTimestamps = null;
        }
        if (mLeaveMerchantTimestamps != null) {
            mLeaveMerchantTimestamps.clear();
            mLeaveMerchantTimestamps = null;
        }

        if (mEnterDetailsTimestamps != null) {
            mEnterDetailsTimestamps.clear();
            mEnterDetailsTimestamps = null;
        }
        if (mLeaveDetailsTimestamps != null) {
            mLeaveDetailsTimestamps.clear();
            mLeaveDetailsTimestamps = null;
        }
    }

}
