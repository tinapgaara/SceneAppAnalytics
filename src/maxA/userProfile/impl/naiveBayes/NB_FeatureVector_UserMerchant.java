package maxA.userProfile.impl.naiveBayes;

import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.impl.GenericFeatureVector;
import maxA.userProfile.impl.GenericFeatureVector_UserMerchant;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

/**
 * Created by TAN on 7/22/2015.
 */
public class NB_FeatureVector_UserMerchant extends GenericFeatureVector_UserMerchant {

    public static final int LENGTH_NB_FeatureVector_UserMerchant = 7;

    public NB_FeatureVector_UserMerchant() {

        super();
    }

    @Override
    protected int getLength() {
        return LENGTH_NB_FeatureVector_UserMerchant;
    }

    @Override
    protected GenericFeatureVector_UserMerchant createFeatureVector_UserMerchant() {

        return new NB_FeatureVector_UserMerchant();
    }

}
