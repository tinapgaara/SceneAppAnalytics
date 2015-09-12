package maxA.userProfile.impl.svm;

import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;

/**
 * Created by TAN on 7/22/2015.
 */
public class SVM_LabeledFeatureVector_UserMerchant extends GenericLabeledFeatureVector_UserMerchant {

    public SVM_LabeledFeatureVector_UserMerchant() {

        super();

        mFeatureVector =  new SVM_FeatureVector_UserMerchant();
    }

    @Override
    protected GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant() {
        return new SVM_LabeledFeatureVector_UserMerchant();
    }
}
