package maxA.userProfile.impl;

import maxA.common.Constants;
import maxA.common.redis.RedisHelper;
import maxA.io.AppLogRecord;
import maxA.userProfile.IOnlineData;
import maxA.userProfile.IOnlineDataEntry;
import maxA.userProfile.feature.userMerchant.UserMerchantFeatureField;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by max2 on 8/13/15.
 */
public abstract class GenericOnlineDataGenerator_UserMerchant implements Serializable {

    protected abstract GenericLabeledFeatureVector_UserMerchant createLabeledFeatureVector_UserMerchant();

    public IOnlineData generateOnlineDataByAppLogs(JavaRDD<AppLogRecord> data) {
        MaxLogger.debug(GenericOnlineDataGenerator_UserMerchant.class, "----------------[generateOnlineDataByAppLogs] BEGIN ...");

        JavaPairRDD<Tuple2<Long,Long>, GenericLabeledFeatureVector_UserMerchant> pairs =
                data.flatMapToPair(new PairFlatMapFunction<AppLogRecord, Tuple2<Long,Long>, GenericLabeledFeatureVector_UserMerchant>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Long,Long>, GenericLabeledFeatureVector_UserMerchant>> call(AppLogRecord appLogRecord) throws Exception {

                        List<Tuple2<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>> res =
                                new ArrayList<Tuple2<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>>();

                        List<Tuple2<Long, Long>> usersAndMerchantsList = appLogRecord.map2UserIdAndMerchantIds(); //appLogRecord.map2UserIdAndMerchantIds

                        for (Tuple2<Long, Long> userIdAndMerchantId : usersAndMerchantsList) {
                            res.add(new Tuple2<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>(userIdAndMerchantId,
                                    convertAppLogRecord2LabeledFeatureVector(appLogRecord)
                            ));
                        }
                        return res;
                    }
                });

        JavaPairRDD<Tuple2<Long,Long>, GenericLabeledFeatureVector_UserMerchant> reducedPairs =
                pairs.reduceByKey(
                        new Function2<GenericLabeledFeatureVector_UserMerchant, GenericLabeledFeatureVector_UserMerchant, GenericLabeledFeatureVector_UserMerchant>() {
                            public GenericLabeledFeatureVector_UserMerchant call(GenericLabeledFeatureVector_UserMerchant lfv_1, GenericLabeledFeatureVector_UserMerchant lfv_2) {
                                return accumulate(lfv_1, lfv_2);
                            }
                        });

        JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> result = reducedPairs.flatMap(
                new FlatMapFunction<Tuple2<Tuple2<Long, Long>, GenericLabeledFeatureVector_UserMerchant>, Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> call(Tuple2<Tuple2<Long, Long>,
                            GenericLabeledFeatureVector_UserMerchant> tuple2) throws Exception {

                        GenericLabeledFeatureVector_UserMerchant lfv = tuple2._2();
                        lfv.setFieldValue(UserMerchantFeatureField.enterM,
                                quantize_enterM(lfv.getEnterMerchantTimeLength()));

                        lfv.setFieldValue(UserMerchantFeatureField.enterDetails,
                                quantize_enterDetails(lfv.getEnterDetailsTimeLength()));

                        List<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> res = new ArrayList<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>>();
                        if ( (tuple2._2() != null) && (tuple2._1() != null) ) {
                            if (tuple2._2().getLabeledPoint() != null) {
                                IOnlineDataEntry dataEntry = new UnifiedOnlineDataEntry<LabeledPoint>(tuple2._2().getLabeledPoint());
                                res.add(new Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>(tuple2._1(), dataEntry));
                            }
                        }
                        return res;
                    }
                });

        IOnlineData onlineDataRes = null;

        if (result == null) {
            MaxLogger.debug(GenericOnlineDataGenerator_UserMerchant.class,
                    "----------------[generateOnlineDataByAppLogs] RESULT-length = NULL " + "----------------");
        }
        else {
            // debugging starts here
            List<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> points = result.collect();
            int resCount = points.size();
            MaxLogger.debug(GenericOnlineDataGenerator_UserMerchant.class,
                    "----------------[generateOnlineDataByLogs] RESULT-length = [" + resCount + "]" + "----------------");

            if (resCount > 0) {

                for (Tuple2<Tuple2<Long, Long>, IOnlineDataEntry> point : points) {
                    Tuple2<Long, Long> indexs = point._1();

                    LabeledPoint dataEntry = ((UnifiedOnlineDataEntry<LabeledPoint>) point._2()).getDataEntry();
                    Vector v = dataEntry.features();
                    double label = dataEntry.label();
                    MaxLogger.debug(GenericOnlineDataGenerator_UserMerchant.class,
                            "[X,y]:" + ", userId " + indexs._1() + ", merchantId:" + indexs._2()+ ": [" + v.apply(0) + " , " +
                                    v.apply(1) + " , " + v.apply(2) + " , " + v.apply(3) + " , " + v.apply(4) + " , " + v.apply(5) + " , " + v.apply(6) + "]"
                                    + ", " + label);
                }
                // debugging ends here
                onlineDataRes = new UnifiedOnlineData(result);
            }
        }
        MaxLogger.debug(GenericOnlineDataGenerator_UserMerchant.class, "---------------- [generateOnlineDataByAppLogs] END ");

        return onlineDataRes;
    }

    public GenericLabeledFeatureVector_UserMerchant convertAppLogRecord2LabeledFeatureVector(AppLogRecord appLogRecord) {
        GenericLabeledFeatureVector_UserMerchant lfv = createLabeledFeatureVector_UserMerchant();

        int action = appLogRecord.getActionName();
        int actionValue;

        switch (action) {
            case AppLogRecord.ACTION_give:
                if (appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_rating))
                    lfv.setLabel(quantizeLabel_give(appLogRecord), appLogRecord.getTimestamp());
                else
                    lfv.setFieldValue(UserMerchantFeatureField.give, quantize_give(appLogRecord));
                break;

            case AppLogRecord.ACTION_reply:
                if (appLogRecord.contextContains(AppLogRecord.CONTEXT_NAME_rating)) {
                    lfv.setLabel(quantizeLabel_reply(appLogRecord), appLogRecord.getTimestamp());
                }
                else
                    lfv.setFieldValue(UserMerchantFeatureField.reply, quantize_reply(appLogRecord));
                break;

            case AppLogRecord.ACTION_ask:
                lfv.setFieldValue(UserMerchantFeatureField.ask, quantize_ask(appLogRecord));
                break;

            case AppLogRecord.ACTION_enter :
                actionValue = appLogRecord.getActionValue();
                if (actionValue == AppLogRecord.ACTION_VAL_merchant) {
                    lfv.addEnterMerchantTimestamp(appLogRecord.getTimestamp());
                }
                else if (actionValue == AppLogRecord.ACTION_VAL_merchantDetails) {
                    lfv.addEnterDetailsTimestamp(appLogRecord.getTimestamp());
                }
                break;

            case AppLogRecord.ACTION_leave :
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
