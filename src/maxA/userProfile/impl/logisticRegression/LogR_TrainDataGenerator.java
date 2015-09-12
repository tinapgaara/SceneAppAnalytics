package maxA.userProfile.impl.logisticRegression;

import maxA.io.AppLogRecord;
import maxA.userProfile.IFeature;
import maxA.userProfile.ITrainData;
import maxA.userProfile.ITrainDataGenerator;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.impl.linearRegression.LR_TrainDataGenerator_UserMerchant;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import org.apache.spark.api.java.JavaRDD;

/**
 * Created by max2 on 7/22/15.
 */
public class LogR_TrainDataGenerator implements ITrainDataGenerator {

    private static LogR_TrainDataGenerator m_instance = null;

    private LogR_TrainDataGenerator() {
        // nothing to do here
    }

    public static LogR_TrainDataGenerator getInstance() {
        if (m_instance == null) {
            m_instance = new LogR_TrainDataGenerator();
        }
        return m_instance;
    }

    public ITrainData generateTrainDataByAppLogs(IFeature feature, JavaRDD<AppLogRecord> data) {
        ITrainData trainData = null;

        String featureName = feature.getName();
        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            trainData = LogR_TrainDataGenerator_UserMerchant.getInstance().generateTrainDataByAppLogs(data);
        }
        else {
            MaxLogger.error(LogR_TrainDataGenerator.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFilter, featureName));
        }

        return trainData;
    }

    @Override
    public ITrainData generateRandomTrainingData(IFeature feature){

        ITrainData trainData = null;

        String featureName = feature.getName();
        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            trainData = LogR_TrainDataGenerator_UserMerchant.getInstance().generateRandomTrainingData();

        }
        else {
            MaxLogger.error(LogR_TrainDataGenerator.class,
                            ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, featureName));
        }

        return trainData;
    }

}
