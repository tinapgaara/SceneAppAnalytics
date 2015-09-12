package maxA.userProfile.feature.userMerchant;

import maxA.userProfile.IFeatureFilter;
import maxA.userProfile.feature.GenericFeature;

/**
 * Created by TAN on 7/5/2015.
 */
public class UserMerchantFeature extends GenericFeature {

    public static final String FEATURE_NAME = "UserMerchant";

    private double mUserMerchantMatrixThreshold;

    public double getUserMerchantThreshold() {
        return mUserMerchantMatrixThreshold;
    }

    public UserMerchantFeature() {

        super(FEATURE_NAME);
        registerField(UserMerchantFeatureField.give);
        registerField(UserMerchantFeatureField.reply);
        registerField(UserMerchantFeatureField.ask);
        registerField(UserMerchantFeatureField.enterM);
        registerField(UserMerchantFeatureField.enterDetails);
        registerField(UserMerchantFeatureField.interest);
        registerField(UserMerchantFeatureField.addM);

        mUserMerchantMatrixThreshold = 0;
    }

}
