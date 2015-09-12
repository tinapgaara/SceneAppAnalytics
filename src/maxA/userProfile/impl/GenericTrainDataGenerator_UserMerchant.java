package maxA.userProfile.impl;

import maxA.common.Constants;
import maxA.common.redis.RedisHelper;
import maxA.io.AppLogRecord;
import maxA.io.sparkClient.SparkContext;
import maxA.userProfile.IFeature;
import maxA.userProfile.ITrainData;
import maxA.userProfile.feature.userMerchant.UserMerchantFeatureField;
import maxA.userProfile.impl.linearRegression.LR_FeatureVector_UserMerchant;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Serializable;
import scala.Tuple2;
import static org.apache.spark.mllib.random.RandomRDDs.*;
//import org.apache.spark.api.JavaDoubleRDD;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by TAN on 8/13/2015.
 */
public abstract class GenericTrainDataGenerator_UserMerchant implements Serializable {

    protected abstract GenericTrainData_UserMerchant createTrainData_UserMerchant(JavaRDD<LabeledPoint> rdd);
    protected abstract GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant();

    protected abstract void setTrainDataNum(int num);
    protected abstract int getTrainDataNum();

    // if the implementation of quantization is not generic at all, we can remove the implementation of these methods and add them as abstract here
    /*
    protected abstract double quantizeLabel_give(AppLogRecord appLogRecord);
    protected abstract double quantizeLabel_reply(AppLogRecord appLogRecord);
    protected abstract double quantize_interest(AppLogRecord appLogRecord);
    protected abstract double quantizeLabel_bookmark(AppLogRecord appLogRecord);
    protected abstract double quantizeLabel_unbookmark(AppLogRecord appLogRecord);
    //*/

    public ITrainData generateTrainDataByAppLogs(JavaRDD<AppLogRecord> data) {

        MaxLogger.info(this.getClass(), "---------------- [generateTrainDataByAppLog] BEGIN ...");

        JavaPairRDD<Tuple2<Long,Long>, GenericLabeledFeatureVector_UserMerchant> pairs =
                data.flatMapToPair(new PairFlatMapFunction<AppLogRecord, Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>>
                    call(AppLogRecord appLogRecord) throws Exception {

                        List<Tuple2<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>> res =
                                new ArrayList<Tuple2<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>>();

                        List<Tuple2<Long, Long>> usersAndMerchantsList = appLogRecord.map2UserIdAndMerchantIds();

                        for (Tuple2<Long, Long> userIdAndMerchantId : usersAndMerchantsList) {
                            res.add(new Tuple2<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>(userIdAndMerchantId,
                                    convertAppLogRecord2LabeledFeatureVector(appLogRecord, userIdAndMerchantId)
                            ));
                        }

                        return res;
                    }
                });

        JavaPairRDD<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant> reducedPairs =
                pairs.reduceByKey(
                        new Function2<GenericLabeledFeatureVector_UserMerchant, GenericLabeledFeatureVector_UserMerchant, GenericLabeledFeatureVector_UserMerchant>() {
                            public GenericLabeledFeatureVector_UserMerchant call(GenericLabeledFeatureVector_UserMerchant lfv_1, GenericLabeledFeatureVector_UserMerchant lfv_2) {
                                return accumulate(lfv_1, lfv_2);
                            }
                        });

        JavaRDD<LabeledPoint> result = reducedPairs.values().map( // TODO: change to flatMap  ?
                new Function<GenericLabeledFeatureVector_UserMerchant, LabeledPoint>() {
                    public LabeledPoint call(GenericLabeledFeatureVector_UserMerchant lfv) {

                        lfv.setFieldValue(UserMerchantFeatureField.enterM,
                                quantize_enterM(lfv.getEnterMerchantTimeLength()));

                        lfv.setFieldValue(UserMerchantFeatureField.enterDetails,
                                quantize_enterDetails(lfv.getEnterDetailsTimeLength()));

                        return lfv.getLabeledPoint();
                    }
                }
        );

        ITrainData trainDataRes = null;
        if (result == null) {
            MaxLogger.debug(this.getClass(),
                    "----------------[generateTrainDataByAppLog] RESULT-length = NULL " + "----------------");
        }
        else {

            // debugging starts here
            List<LabeledPoint> points = result.collect();
            int pointsSize = points.size();
            MaxLogger.debug(this.getClass(),
                    "----------------[generateTrainDataByAppLog] RESULT-length = [" + points.size() + "]"+ "----------------");

            if (pointsSize > 0) {
                for (LabeledPoint point : points) {
                    Vector v = point.features();
                    MaxLogger.debug(this.getClass(),
                            "[X,y]:[" + v.apply(0) + " , " + v.apply(1) + " , " + v.apply(2) + " , " + v.apply(3) + " , " +
                                    v.apply(4) + " , " + v.apply(5) + " , " + v.apply(6) + "]" + ", " + point.label());
                }
            }
            // debugging ends here

            trainDataRes = createTrainData_UserMerchant(result);
        }

        MaxLogger.debug(this.getClass(), "---------------- [generateTrainDataByAppLog] END ");

        return trainDataRes;
    }

    public ITrainData generateRandomTrainingData() {

        SparkContext.setJscStartFlag();
        JavaSparkContext jsc = SparkContext.getSparkContext();
        JavaDoubleRDD randomPoints = normalJavaRDD(jsc, Constants.GeneratedTrainDataNum, 10);

        GenericLabeledFeatureVector_UserMerchant lfv = createLabeledFeatureVector_UserMerchant();
        final int vectorLen = lfv.mFeatureVector.getLength();
        final int len = vectorLen + 1;

        final double threshold = Constants.TrainDataGenerationThreshold;

        MaxLogger.info(this.getClass(), "---------------- [generateRandomTrainingData] BEGIN ...");

        JavaPairRDD<Tuple2<Integer, Integer>, LabeledPoint> pairs = randomPoints.mapToPair(new PairFunction<Double, Tuple2<Integer, Integer>, LabeledPoint>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, LabeledPoint> call(Double value) {

                double newVal = 0;
                if (value > threshold) {
                    newVal = 1;
                }

                Vector vector = null;
                LabeledPoint point = null;

                int currentDataNo = getTrainDataNum();
                int id = currentDataNo / len;
                int vectorIndice = currentDataNo % len;

                int[] indices = new int[vectorLen];
                double[] values = new double[vectorLen];
                for (int i = 0 ; i < vectorLen ; i ++) {
                    indices[i] = i;
                    values[i] = 0;
                }

                double Label = 0;
                if (vectorIndice < vectorLen) {
                    for (int i = 0; i < values.length; i++) {
                        if (i == vectorIndice) {
                            values[i] = newVal;
                        }
                    }
                } else {
                    Label = newVal;
                }

                vector = Vectors.sparse(vectorLen, indices, values);
                point = new LabeledPoint(Label, vector);

                currentDataNo ++;
                setTrainDataNum(currentDataNo);

                return new Tuple2<Tuple2<Integer, Integer>, LabeledPoint>(new Tuple2<Integer, Integer>(id, id), point);
            }
        });

        JavaPairRDD<Tuple2<Integer, Integer>, LabeledPoint> reducedPairs = pairs.reduceByKey(new Function2<LabeledPoint, LabeledPoint, LabeledPoint>() {
            @Override
            public LabeledPoint call(LabeledPoint labeledPoint, LabeledPoint labeledPoint2) throws Exception {
                Vector v_1 = labeledPoint.features();
                Vector v_2 = labeledPoint2.features();
                double label_1 = labeledPoint.label();
                double label_2 = labeledPoint2.label();

                int len = v_1.size();
                int len_2 = v_2.size();
                if (len != len_2) {
                    MaxLogger.error(GenericTrainDataGenerator_UserMerchant.class,
                                    ErrMsg.ERR_MSG_IncompatibleFeatureVector);
                }

                int[] indices = new int[len];
                for (int i = 0 ; i < vectorLen ; i ++) {
                    indices[i] = i;
                }
                double[] values = new double[len];
                for (int i = 0; i < v_1.size() ; i ++) {
                    values[i] = v_1.apply(i) + v_2.apply(i);
                }
                Vector newVector = Vectors.sparse(len, indices, values);
                double newLabel = label_1 + label_2;

                return new LabeledPoint(newLabel, newVector);
            }
        });

        ITrainData trainDataRes = null;
        JavaRDD<LabeledPoint> result = reducedPairs.values();

        if (result == null) {
            MaxLogger.error(GenericTrainDataGenerator_UserMerchant.class,
                            ErrMsg.ERR_MSG_NullTrainData);
        }
        else {
            // debugging starts here
            List<LabeledPoint> points = result.collect();
            int pointsSize = points.size();
            MaxLogger.debug(this.getClass(),
                    "----------------[generateRandomTrainingData] RESULT-length = [" + points.size() + "]"+ "----------------");

            if (pointsSize > 0) {
                for (LabeledPoint point : points) {
                    Vector v = point.features();
                    MaxLogger.debug(this.getClass(),
                            "[X,y]:[" + v.apply(0) + " , " + v.apply(1) + " , " + v.apply(2) + " , " + v.apply(3) + " , " +
                                    v.apply(4) + " , " + v.apply(5) + " , " + v.apply(6) + "]" + ", " + point.label());
                }
            }
            // debugging ends here

            trainDataRes = createTrainData_UserMerchant(result);
        }

        MaxLogger.debug(this.getClass(), "---------------- [generateRandomTrainingData] END ");
        return trainDataRes;
    }

    protected GenericLabeledFeatureVector_UserMerchant convertAppLogRecord2LabeledFeatureVector(AppLogRecord appLogRecord, Tuple2<Long, Long> userIdAndMerchantId) {

        GenericLabeledFeatureVector_UserMerchant lfv = createLabeledFeatureVector_UserMerchant();

        int action = appLogRecord.getActionName();
        int actionValue;
        switch (action) {
            case AppLogRecord.ACTION_give:
                if (appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_rating)) {
                    lfv.setLabel(quantizeLabel_give(appLogRecord), appLogRecord.getTimestamp());
                }
                else {
                    lfv.setFieldValue(UserMerchantFeatureField.give, quantize_give(appLogRecord));
                }
                break;

            case AppLogRecord.ACTION_reply:
                if (appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_rating)) {
                    lfv.setLabel(quantizeLabel_reply(appLogRecord), appLogRecord.getTimestamp());
                }
                else {
                    lfv.setFieldValue(UserMerchantFeatureField.reply, quantize_reply(appLogRecord));
                }
                break;

            case AppLogRecord.ACTION_ask:
                lfv.setFieldValue(UserMerchantFeatureField.ask, quantize_ask(appLogRecord));
                break;

            case AppLogRecord.ACTION_enter:
                actionValue = appLogRecord.getActionValue();
                if (actionValue == AppLogRecord.ACTION_VAL_merchant) {
                    lfv.addEnterMerchantTimestamp(appLogRecord.getTimestamp());
                }
                else if (actionValue == AppLogRecord.ACTION_VAL_merchantDetails) {
                    lfv.addEnterDetailsTimestamp(appLogRecord.getTimestamp());
                }
                break;

            case AppLogRecord.ACTION_leave:
                actionValue = appLogRecord.getActionValue();
                if (actionValue == AppLogRecord.ACTION_VAL_merchant) {
                    lfv.addLeaveMerchantTimestamp(appLogRecord.getTimestamp());
                }
                else if (actionValue == AppLogRecord.ACTION_VAL_merchantDetails) {
                    lfv.addLeaveDetailsTimestamp(appLogRecord.getTimestamp());
                }
                break;

            case AppLogRecord.ACTION_interest:
                lfv.setFieldValue(UserMerchantFeatureField.interest, quantize_interest(appLogRecord));
                break;

            case AppLogRecord.ACTION_add:
                lfv.setFieldValue(UserMerchantFeatureField.addM, quantize_addM(appLogRecord));
                break;

            case AppLogRecord.ACTION_bookmark:
                lfv.setLabel(quantizeLabel_bookmark(appLogRecord), appLogRecord.getTimestamp());
                break;

            case AppLogRecord.ACTION_unbookmark:
                lfv.setLabel(quantizeLabel_unbookmark(appLogRecord), appLogRecord.getTimestamp());
                break;

            case AppLogRecord.ACTION_endorse:
                lfv.setLabel(quantizeLabel_endorse(appLogRecord), appLogRecord.getTimestamp());
                break;

            case AppLogRecord.ACTION_vote:
                lfv.setLabel(quantizeLabel_vote(appLogRecord), appLogRecord.getTimestamp());
                break;
        }

        return lfv;
    }

    protected GenericLabeledFeatureVector_UserMerchant accumulate(
            GenericLabeledFeatureVector_UserMerchant lfv_1,
            GenericLabeledFeatureVector_UserMerchant lfv_2) {

        return lfv_1.combine(lfv_2);
    }

    protected double quantize_give(AppLogRecord appLogRecord) {

        if (appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_queue) ||
                appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_atmosphere) ) {
            return 1;
        }

        return 0;
    }

    protected double quantizeLabel_give(AppLogRecord appLogRecord) {

        long rating = appLogRecord.getContextValue(AppLogRecord.CONTEXT_NAME_rating);
        if (rating == AppLogRecord.CONTEXT_VAL_good) {
            return 1;
        }
        else if (rating == AppLogRecord.CONTEXT_VAL_bad) {
            return -1;
        }

        return 0;
    }

    protected double quantize_reply(AppLogRecord appLogRecord) {

        if (appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_queue) ||
                appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_atmosphere) ||
                appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_specials) ) {
            return 1;
        }

        return 0;
    }

    protected double quantizeLabel_reply(AppLogRecord appLogRecord) {

        long rating = appLogRecord.getContextValue(AppLogRecord.CONTEXT_NAME_rating);
        if (rating == AppLogRecord.CONTEXT_VAL_good) {
            return 1;
        }
        else if (rating == AppLogRecord.CONTEXT_VAL_bad) {
            return -1;
        }

        return 0;
    }

    protected double quantize_ask(AppLogRecord appLogRecord) {

        return 1;
    }

    protected double quantize_enterM(double timeLength) {

        if (timeLength > Constants.THRESHOLD_EnterMerchantTimeLength_InSeconds) {
            return 1;
        }

        return 0;
    }

    protected double quantize_enterDetails(double timeLength) {

        if (timeLength > Constants.THRESHOLD_EnterDetailsTimeLength_InSeconds) {
            return 1;
        }

        return 0;
    }

    protected double quantize_interest(AppLogRecord appLogRecord) {


        int rating = appLogRecord.getActionValue();
        if (rating == AppLogRecord.ACTION_VAL_add) {
            return 1;
        }
        else if (rating == AppLogRecord.ACTION_VAL_remove) {
            return -1;
        }
        return 0;
    }

    protected double quantize_addM(AppLogRecord appLogRecord) {

        return 1;
    }

    protected double quantizeLabel_bookmark(AppLogRecord appLogRecord) {

        return 1;
    }

    protected double quantizeLabel_unbookmark(AppLogRecord appLogRecord) {

        return -1;
    }

    protected double quantizeLabel_endorse(AppLogRecord appLogRecord) {

        int actionValue = appLogRecord.getActionValue();

        long giveId = appLogRecord.getActionId();
        Long merchantID = appLogRecord.getContextValue(AppLogRecord.CONTEXT_NAME_merchantId);
        Long giverID = appLogRecord.getContextValue(AppLogRecord.CONTEXT_NAME_giverId);
        if ( (merchantID == -1) || (giverID == -1) ) {
            MaxLogger.error(this.getClass(),
                    ErrMsg.ERR_MSG_IllegalEndorse);
        }

        RedisHelper redisHelper = RedisHelper.getInstance();
        String rating = redisHelper.getRatingByGiveId(giverID, merchantID, giveId);
        if ( ( (rating.equals("Good"))
                && (actionValue == AppLogRecord.ACTION_VAL_true) )
                || ( (rating.equals("Bad"))
                && (actionValue == AppLogRecord.ACTION_VAL_false) ) ) {

            return 1;
        }
        else if ( ( (rating.equals("Bad"))
                && (actionValue == AppLogRecord.ACTION_VAL_true) )
                || ( (rating.equals("Good"))
                && (actionValue == AppLogRecord.ACTION_VAL_false) ) ) {

            return -1;
        }

        return 0;
    }

    protected double quantizeLabel_vote(AppLogRecord appLogRecord) {

        int actionVal = appLogRecord.getActionValue();

        if (actionVal == AppLogRecord.ACTION_VAL_true) {
            return 1;
        }
        else if (actionVal == AppLogRecord.ACTION_VAL_false) {
            return -1;
        }

        return 0;
    }

}
