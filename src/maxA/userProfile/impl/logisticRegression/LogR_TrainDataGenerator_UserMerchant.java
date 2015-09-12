package maxA.userProfile.impl.logisticRegression;

import maxA.io.AppLogRecord;
import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;
import maxA.userProfile.impl.GenericTrainDataGenerator_UserMerchant;
import maxA.userProfile.impl.GenericTrainData_UserMerchant;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Created by TAN on 7/22/2015.
 */
public class LogR_TrainDataGenerator_UserMerchant extends GenericTrainDataGenerator_UserMerchant {

    private static LogR_TrainDataGenerator_UserMerchant m_instance = null;

    protected static int mGeneratedTrainDataNum;

    public static LogR_TrainDataGenerator_UserMerchant getInstance() {

        if (m_instance == null) {
            m_instance = new LogR_TrainDataGenerator_UserMerchant();
        }
        return m_instance;
    }

    private LogR_TrainDataGenerator_UserMerchant() {

        mGeneratedTrainDataNum = 0;
    }

    protected void setTrainDataNum(int num) {

        mGeneratedTrainDataNum = num;
    }

    protected int getTrainDataNum() {

        return mGeneratedTrainDataNum;
    }

    protected GenericTrainData_UserMerchant createTrainData_UserMerchant(JavaRDD<LabeledPoint> rdd) {
        return new LogR_TrainData_UserMerchant(rdd);
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
