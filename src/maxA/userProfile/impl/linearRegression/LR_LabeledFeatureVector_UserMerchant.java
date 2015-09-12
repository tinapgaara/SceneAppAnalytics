package maxA.userProfile.impl.linearRegression;

import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;

/**
 * Created by TAN on 7/19/2015.
 */
public class LR_LabeledFeatureVector_UserMerchant extends GenericLabeledFeatureVector_UserMerchant {

    public LR_LabeledFeatureVector_UserMerchant() {

        super();

        mFeatureVector = new LR_FeatureVector_UserMerchant();
    }

    @Override
    protected GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant() {

        return new LR_LabeledFeatureVector_UserMerchant();
    }

}
