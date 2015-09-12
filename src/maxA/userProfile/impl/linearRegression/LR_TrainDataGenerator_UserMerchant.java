package maxA.userProfile.impl.linearRegression;

import maxA.common.Constants;
import maxA.common.redis.RedisHelper;
import maxA.io.AppLogRecord;
import maxA.userProfile.ITrainData;
import maxA.userProfile.feature.userMerchant.*;
import maxA.userProfile.impl.GenericLabeledFeatureVector_UserMerchant;
import maxA.userProfile.impl.GenericTrainDataGenerator_UserMerchant;
import maxA.userProfile.impl.GenericTrainData_UserMerchant;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by max2 on 7/17/15.
 */
public class LR_TrainDataGenerator_UserMerchant extends GenericTrainDataGenerator_UserMerchant {

    private static LR_TrainDataGenerator_UserMerchant m_instance = null;

    protected static int mGeneratedTrainDataNum;

    public static LR_TrainDataGenerator_UserMerchant getInstance() {

        if (m_instance == null) {
            m_instance = new LR_TrainDataGenerator_UserMerchant();
        }

        return m_instance;
    }

    private LR_TrainDataGenerator_UserMerchant() {

        mGeneratedTrainDataNum = 0;
    }

    protected void setTrainDataNum(int num) {

        mGeneratedTrainDataNum = num;
    }

    protected int getTrainDataNum() {

        return mGeneratedTrainDataNum;
    }

    protected GenericTrainData_UserMerchant createTrainData_UserMerchant(JavaRDD<LabeledPoint> rdd) {
        return new LR_TrainData_UserMerchant(rdd);
    }

    protected GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant() {
        return new LR_LabeledFeatureVector_UserMerchant();
    }

}
