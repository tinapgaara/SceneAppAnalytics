package maxA.userProfile.impl.svm;

import maxA.io.AppLogRecord;
import maxA.userProfile.IFeature;
import maxA.userProfile.IOnlineData;
import maxA.userProfile.IOnlineDataGenerator;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.feature.userUser.UserUserFeature;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Created by max2 on 8/10/15.
 */
public class SVM_OnlineDataGenerator implements IOnlineDataGenerator, Serializable {

    private static SVM_OnlineDataGenerator m_instance = null;

    public static SVM_OnlineDataGenerator getInstance() {

        if (m_instance == null) {
            m_instance = new SVM_OnlineDataGenerator();
        }
        return m_instance;
    }

    protected SVM_OnlineDataGenerator() {
        // nothing to do here
    }

    @Override
    public IOnlineData generateOnlineDataByAppLogs(IFeature feature,  JavaRDD<AppLogRecord> data) {

        IOnlineData testData = null;

        String featureName = feature.getName();

        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            testData = SVM_OnlineDataGenerator_UserMerchant.getInstance().generateOnlineDataByAppLogs(data);
        }
        else if (featureName.equals(UserUserFeature.FEATURE_NAME)) {
            testData = SVM_OnlineDataGenerator_UserUser.getInstance().generateOnlineDataByAppLogs(data);
        }
        else {
            MaxLogger.error(SVM_OnlineDataGenerator.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFilter, featureName));
        }

        return testData;
    }
}

