package maxA.userProfile.impl.logisticRegression;

import maxA.io.AppLogRecord;
import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;
import maxA.userProfile.impl.GenericOnlineDataGenerator_UserMerchant;
/**
 * Created by max2 on 8/10/15.
 */
public class LogR_OnlineDataGenerator_UserMerchant extends GenericOnlineDataGenerator_UserMerchant {

    private static LogR_OnlineDataGenerator_UserMerchant m_instance = null;

    public static LogR_OnlineDataGenerator_UserMerchant getInstance() {

        if (m_instance == null) {
            m_instance = new LogR_OnlineDataGenerator_UserMerchant();
        }
        return m_instance;
    }

    private LogR_OnlineDataGenerator_UserMerchant() {
        // nothing to do here
    }

    protected GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant() {
        return new LogR_LabeledFeatureVector_UserMerchant();
    }

    @Override
    protected double quantizeLabel_give(AppLogRecord appLogRecord) {

        return super.quantizeLabel_give(appLogRecord) + 1;
    }

    @Override
    protected double quantizeLabel_reply(AppLogRecord appLogRecord) {

        return super.quantizeLabel_reply(appLogRecord) + 1;
    }

    @Override
    protected double quantize_interest(AppLogRecord appLogRecord) {

        return super.quantize_interest(appLogRecord) + 1;
    }

    @Override
    protected double quantizeLabel_bookmark(AppLogRecord appLogRecord) {

        return super.quantizeLabel_bookmark(appLogRecord) + 1;
    }

    @Override
    protected double quantizeLabel_unbookmark(AppLogRecord appLogRecord) {

        return super.quantizeLabel_unbookmark(appLogRecord) + 1;
    }

    @Override
    protected double quantizeLabel_endorse(AppLogRecord appLogRecord) {

        return super.quantizeLabel_endorse(appLogRecord) + 1;
    }

    @Override
    protected double quantizeLabel_vote(AppLogRecord appLogRecord) {

        return super.quantizeLabel_vote(appLogRecord) + 1;
    }
}

