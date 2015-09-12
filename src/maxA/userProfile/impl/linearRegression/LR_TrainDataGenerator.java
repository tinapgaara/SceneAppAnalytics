package maxA.userProfile.impl.linearRegression;

import maxA.io.AppLogRecord;
import maxA.io.IRecord;
import maxA.io.decoder.IDecoder;
import maxA.userProfile.IFeature;
import maxA.userProfile.ITrainData;
import maxA.userProfile.ITrainDataGenerator;
import maxA.userProfile.feature.userMerchant.*;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Created by TAN on 7/5/2015.
 */
public class LR_TrainDataGenerator implements ITrainDataGenerator, Serializable {

    protected static LR_TrainDataGenerator m_instance = null;

    protected LR_TrainDataGenerator() {
        // nothing to do here
    }

    public static LR_TrainDataGenerator getInstance() {

        if (m_instance == null) {
            m_instance = new LR_TrainDataGenerator();
        }
        return m_instance;
    }

    @Override
    public ITrainData generateTrainDataByAppLogs(IFeature feature, JavaRDD<AppLogRecord> data) {

        ITrainData trainData = null;

        String featureName = feature.getName();
        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            trainData = LR_TrainDataGenerator_UserMerchant.getInstance().generateTrainDataByAppLogs(data);

        }
        else {
            MaxLogger.error(LR_TrainDataGenerator.class,
                            ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, featureName));
        }

        return trainData;
    }

    @Override
    public ITrainData generateRandomTrainingData(IFeature feature){

        ITrainData trainData = null;

        String featureName = feature.getName();
        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            trainData = LR_TrainDataGenerator_UserMerchant.getInstance().generateRandomTrainingData();

        }
        else {
            MaxLogger.error(LR_TrainDataGenerator.class,
                            ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, featureName));
        }

        return trainData;
    }

}
