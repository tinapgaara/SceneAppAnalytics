package maxA.userProfile.impl.linearRegression;

import maxA.userProfile.impl.GenericFeatureVector_UserMerchant;

/**
 * Created by TAN on 7/6/2015.
 */
public class LR_FeatureVector_UserMerchant extends GenericFeatureVector_UserMerchant {

    private static final int LENGTH_LR_FeatureVector_UserMerchant = 7;

    public LR_FeatureVector_UserMerchant() {

        super();
    }

    @Override
    protected int getLength() {
        return LENGTH_LR_FeatureVector_UserMerchant;
    }

    @Override
    protected GenericFeatureVector_UserMerchant createFeatureVector_UserMerchant() {

        return new LR_FeatureVector_UserMerchant();
    }

}
