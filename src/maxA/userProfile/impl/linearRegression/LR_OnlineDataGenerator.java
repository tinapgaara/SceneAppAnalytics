package maxA.userProfile.impl.linearRegression;

import maxA.io.AppLogRecord;
import maxA.userProfile.*;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.feature.userUser.UserUserFeature;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by TAN on 7/6/2015.
 */
public class LR_OnlineDataGenerator implements IOnlineDataGenerator, Serializable {

    private static LR_OnlineDataGenerator m_instance = null;

    public static LR_OnlineDataGenerator getInstance() {

        if (m_instance == null) {
            m_instance = new LR_OnlineDataGenerator();
        }
        return m_instance;
    }

    protected LR_OnlineDataGenerator() {
        // nothing to do here
    }

    @Override
    public IOnlineData generateOnlineDataByAppLogs(IFeature feature,  JavaRDD<AppLogRecord> data) {

        IOnlineData testData = null;

        String featureName = feature.getName();

        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            testData = LR_OnlineDataGenerator_UserMerchant.getInstance().generateOnlineDataByAppLogs(data);
        }
        else if (featureName.equals(UserUserFeature.FEATURE_NAME)) {
            testData = LR_OnlineDataGenerator_UserUser.getInstance().generateOnlineDataByAppLogs(data);
        }
        else {
            MaxLogger.error(LR_OnlineDataGenerator.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFilter, featureName));
        }

        return testData;
    }
}
