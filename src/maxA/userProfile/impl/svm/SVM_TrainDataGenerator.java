package maxA.userProfile.impl.svm;

import maxA.io.AppLogRecord;
import maxA.io.IRecord;
import maxA.userProfile.IFeature;
import maxA.userProfile.ITrainData;
import maxA.userProfile.ITrainDataGenerator;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by max2 on 7/22/15.
 */
public class SVM_TrainDataGenerator implements ITrainDataGenerator {

    private static SVM_TrainDataGenerator m_instance = null;

    private SVM_TrainDataGenerator() {
        // nothing to do here
    }

    public static SVM_TrainDataGenerator getInstance() {
        if (m_instance == null) {
            m_instance = new SVM_TrainDataGenerator();
        }
        return m_instance;
    }

    public ITrainData generateTrainDataByAppLogs(IFeature feature, JavaRDD<AppLogRecord> data) {

        ITrainData trainData = null;

        String featureName = feature.getName();
        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            trainData = SVM_TrainDataGenerator_UserMerchant.getInstance().generateTrainDataByAppLogs(data);
        }
        else {
            MaxLogger.error(SVM_TrainDataGenerator.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFilter, featureName));
        }

        return trainData;
    }

    @Override
    public ITrainData generateRandomTrainingData(IFeature feature){

        ITrainData trainData = null;

        String featureName = feature.getName();
        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            trainData = SVM_TrainDataGenerator_UserMerchant.getInstance().generateRandomTrainingData();

        }
        else {
            MaxLogger.error(SVM_TrainDataGenerator.class,
                            ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, featureName));
        }

        return trainData;
    }
}
