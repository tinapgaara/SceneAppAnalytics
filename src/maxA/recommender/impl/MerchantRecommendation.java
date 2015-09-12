package maxA.recommender.impl;

import maxA.recommender.IExplaination;
import maxA.recommender.IMerchantRecommendation;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by max2 on 7/29/15.
 */
public class MerchantRecommendation implements IMerchantRecommendation, Serializable {

    private long mMerchantId;
    private double mLikeDegree;
    private double mConfidenceDegree;
    private IExplaination mExplaination;

    public MerchantRecommendation() {

        mMerchantId = -1;
        mLikeDegree = 0 ;
        mConfidenceDegree = 0;
        mExplaination = null;
    }

    public MerchantRecommendation(long merchantId, double likeDegree) {

        mMerchantId = merchantId;
        mLikeDegree = likeDegree;
        mConfidenceDegree = 0;
        mExplaination = null;
    }

    @Override
    public long getMerchantId() {

        return mMerchantId;
    }

    public void setMerchantId(long merchantId) {

        mMerchantId = merchantId;
    }

    @Override
    public double getLikeDegree() {

        return mLikeDegree;
    }

    public void setLikeDegree(double likeDegree) {

        mLikeDegree = likeDegree;
    }

    @Override
    public double getConfidenceDegree() {

        return mConfidenceDegree;
    }

    @Override
    public IExplaination getExplaination() {

        return mExplaination;
    }

    public static class MerchantRecommendationComparators implements Comparator<IMerchantRecommendation> {

        @Override
        public int compare(IMerchantRecommendation s1,IMerchantRecommendation s2){
            if ( (s1 != null) && (s2 != null) ) {
                if (s1.getLikeDegree() > s2.getLikeDegree()) {
                    return -1;
                }
                else if (s1.getLikeDegree() < s2.getLikeDegree()) {
                    return 1;
                }
            }
            return 0;
        }
    }
}
