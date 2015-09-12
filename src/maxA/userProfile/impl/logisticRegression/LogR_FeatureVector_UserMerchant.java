package maxA.userProfile.impl.logisticRegression;

import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.impl.GenericFeatureVector_UserMerchant;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

/**
 * Created by TAN on 7/22/2015.
 */
public class LogR_FeatureVector_UserMerchant extends GenericFeatureVector_UserMerchant {

    private static final int LENGTH_LogR_FeatureVector_UserMerchant = 7;

    public LogR_FeatureVector_UserMerchant() {

        super();
    }

    @Override
    protected int getLength() {
        return LENGTH_LogR_FeatureVector_UserMerchant;
    }

    @Override
    protected GenericFeatureVector_UserMerchant createFeatureVector_UserMerchant() {

        return new LogR_FeatureVector_UserMerchant();
    }

}
