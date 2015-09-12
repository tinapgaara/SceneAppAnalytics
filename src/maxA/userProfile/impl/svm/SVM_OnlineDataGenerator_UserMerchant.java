package maxA.userProfile.impl.svm;

import maxA.io.AppLogRecord;
import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;
import maxA.userProfile.impl.GenericOnlineDataGenerator_UserMerchant;
/**
 * Created by max2 on 8/10/15.
 */
public class SVM_OnlineDataGenerator_UserMerchant extends GenericOnlineDataGenerator_UserMerchant {

    private static SVM_OnlineDataGenerator_UserMerchant m_instance = null;

    public static SVM_OnlineDataGenerator_UserMerchant getInstance() {

        if (m_instance == null) {
            m_instance = new SVM_OnlineDataGenerator_UserMerchant();
        }
        return m_instance;
    }

    private SVM_OnlineDataGenerator_UserMerchant() {
        // nothing to do here
    }

    protected GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant() {
        return new SVM_LabeledFeatureVector_UserMerchant();
    }

    @Override
    protected double quantizeLabel_give(AppLogRecord appLogRecord) {

        double result = super.quantizeLabel_give(appLogRecord);
        if (result < 0) {
            result = 0;
        }

        return result;
    }

    @Override
    protected double quantizeLabel_reply(AppLogRecord appLogRecord) {

        double result = super.quantizeLabel_reply(appLogRecord);
        if (result < 0) {
            result = 0;
        }

        return result;
    }

    @Override
    protected double quantize_interest(AppLogRecord appLogRecord) {

        double result = super.quantize_interest(appLogRecord);
        if (result < 0) {
            result = 0;
        }

        return result;
    }

    @Override
    protected double quantizeLabel_unbookmark(AppLogRecord appLogRecord) {

        return 0;
    }

    @Override
    protected double quantizeLabel_endorse(AppLogRecord appLogRecord) {

        double result = super.quantizeLabel_endorse(appLogRecord);
        if (result < 0) {
            result = 0;
        }

        return result;
    }

    protected double quantizeLabel_vote(AppLogRecord appLogRecord) {

        double result = super.quantizeLabel_vote(appLogRecord);
        if (result < 0) {
            result = 0;
        }

        return result;
    }
}
