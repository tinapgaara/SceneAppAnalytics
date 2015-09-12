package maxA.userProfile.impl.logisticRegression;

import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;

/**
 * Created by TAN on 7/22/2015.
 */
public class LogR_LabeledFeatureVector_UserMerchant extends GenericLabeledFeatureVector_UserMerchant {

    public LogR_LabeledFeatureVector_UserMerchant() {

        super();

        mFeatureVector = new LogR_FeatureVector_UserMerchant();
    }

    @Override
    protected GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant() {

        return new LogR_LabeledFeatureVector_UserMerchant();
    }
}
