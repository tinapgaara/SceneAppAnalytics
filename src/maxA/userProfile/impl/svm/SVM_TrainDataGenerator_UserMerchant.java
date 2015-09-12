package maxA.userProfile.impl.svm;

import maxA.io.AppLogRecord;
import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;
import maxA.userProfile.impl.GenericTrainDataGenerator_UserMerchant;
import maxA.userProfile.impl.GenericTrainData_UserMerchant;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Created by TAN on 7/22/2015.
 */
public class SVM_TrainDataGenerator_UserMerchant extends GenericTrainDataGenerator_UserMerchant {

    private static SVM_TrainDataGenerator_UserMerchant m_instance = null;

    protected static int mGeneratedTrainDataNum;

    public static SVM_TrainDataGenerator_UserMerchant getInstance() {

        if (m_instance == null) {
            m_instance = new SVM_TrainDataGenerator_UserMerchant();
        }
        return m_instance;
    }

    private SVM_TrainDataGenerator_UserMerchant() {
        mGeneratedTrainDataNum = 0;
    }

    protected void setTrainDataNum(int num) {

        mGeneratedTrainDataNum = num;
    }

    protected int getTrainDataNum() {

        return mGeneratedTrainDataNum;
    }

    protected GenericTrainData_UserMerchant createTrainData_UserMerchant(JavaRDD<LabeledPoint> rdd) {
        return new SVM_TrainData_UserMerchant(rdd);
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
