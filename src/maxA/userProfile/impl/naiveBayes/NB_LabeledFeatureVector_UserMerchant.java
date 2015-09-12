package maxA.userProfile.impl.naiveBayes;

import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;

/**
 * Created by TAN on 7/22/2015.
 */
public class NB_LabeledFeatureVector_UserMerchant extends GenericLabeledFeatureVector_UserMerchant {

    public NB_LabeledFeatureVector_UserMerchant() {

        super();

        mFeatureVector = new NB_FeatureVector_UserMerchant();
    }

    @Override
    protected GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant() {

        return new NB_LabeledFeatureVector_UserMerchant();
    }

}
