package maxA.userProfile.impl.svm;

import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.impl.GenericFeatureVector;
import maxA.userProfile.impl.GenericFeatureVector_UserMerchant;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

/**
 * Created by max2 on 7/22/15.
 */
public class SVM_FeatureVector_UserMerchant extends GenericFeatureVector_UserMerchant {

    public static final int LENGTH_SVM_FeatureVector_UserMerchant = 7;

    public SVM_FeatureVector_UserMerchant() {

        super();
    }

    @Override
    protected int getLength() {
        return LENGTH_SVM_FeatureVector_UserMerchant;
    }

    @Override
    protected GenericFeatureVector_UserMerchant createFeatureVector_UserMerchant() {
        return new SVM_FeatureVector_UserMerchant();
    }

}
