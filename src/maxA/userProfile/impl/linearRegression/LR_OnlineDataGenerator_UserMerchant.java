package maxA.userProfile.impl.linearRegression;

import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;
import maxA.userProfile.impl.GenericOnlineDataGenerator_UserMerchant;

/**
 * Created by max2 on 7/24/15.
 */
public class LR_OnlineDataGenerator_UserMerchant extends GenericOnlineDataGenerator_UserMerchant {

    private static LR_OnlineDataGenerator_UserMerchant m_instance = null;

    public static LR_OnlineDataGenerator_UserMerchant getInstance() {

        if (m_instance == null) {
            m_instance = new LR_OnlineDataGenerator_UserMerchant();
        }
        return m_instance;
    }

    private LR_OnlineDataGenerator_UserMerchant() {
        // nothing to do here
    }

    protected GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant() {
        return new LR_LabeledFeatureVector_UserMerchant();
    }
}
