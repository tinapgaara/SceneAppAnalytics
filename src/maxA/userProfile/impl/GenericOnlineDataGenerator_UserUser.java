package maxA.userProfile.impl;

import maxA.common.redis.RedisHelper;
import maxA.io.AppLogRecord;
import maxA.userProfile.IOnlineData;
import maxA.userProfile.IOnlineDataEntry;
import maxA.userProfile.feature.userUser.UserUserFeatureField;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by max2 on 8/13/15.
 */
public abstract class GenericOnlineDataGenerator_UserUser implements Serializable {

    protected abstract GenericFeatureVector_UserUser createFeatureVector_UserUser();

    public IOnlineData generateOnlineDataByAppLogs(JavaRDD<AppLogRecord> data) {

        MaxLogger.info(GenericOnlineDataGenerator_UserUser.class, "----------------[generateOnlineDataByAppLogs] BEGIN ...");

        JavaPairRDD<Tuple2<Long,Long>, GenericFeatureVector_UserUser> pairs =
                data.flatMapToPair(new PairFlatMapFunction<AppLogRecord, Tuple2<Long,Long>, GenericFeatureVector_UserUser>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Long,Long>, GenericFeatureVector_UserUser>> call(AppLogRecord appLogRecord) throws Exception {
                        List<Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser>> res = convertAppLogRecord2Tuple2(appLogRecord);
                        return res;
                    }
                });

        JavaPairRDD<Tuple2<Long,Long>, GenericFeatureVector_UserUser> reducedPairs =
                pairs.reduceByKey(
                        new Function2<GenericFeatureVector_UserUser, GenericFeatureVector_UserUser, GenericFeatureVector_UserUser>() {
                            public GenericFeatureVector_UserUser call(GenericFeatureVector_UserUser lfv_1, GenericFeatureVector_UserUser lfv_2) {
                                return accumulate(lfv_1, lfv_2);
                            }
                        });

        JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> result = reducedPairs.flatMap(
                new FlatMapFunction<Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser>, Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> call(Tuple2<Tuple2<Long, Long>,
                            GenericFeatureVector_UserUser> tuple2) throws Exception {

                        List<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> res = new ArrayList<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>>();
                        if ((tuple2._2() != null) && (tuple2._1() != null)) {
                            if (tuple2._2().getData() != null) {
                                IOnlineDataEntry dataEntry = new UnifiedOnlineDataEntry<Vector>(tuple2._2().getData());
                                res.add(new Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>(tuple2._1(), dataEntry));
                            }
                        }
                        return res;
                    }
                });

        IOnlineData onlineDataRes = null;

        if (result == null) {
            MaxLogger.info(GenericOnlineDataGenerator_UserUser.class,
                            "----------------[generateOnlineDataByAppLogs] RESULT-length = NULL " + "----------------");
        }
        else {
            // debugging starts here
            List<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> points = result.collect();
            int resCount = points.size();
            MaxLogger.debug(GenericOnlineDataGenerator_UserUser.class,
                            "----------------[generateOnlineDataByAppLogs] RESULT-length = [" + resCount + "]" + "----------------");

            if (resCount > 0) {

                for (Tuple2<Tuple2<Long, Long>, IOnlineDataEntry> point : points) {
                    Tuple2<Long, Long> indexs = point._1();
                    Vector v = ((UnifiedOnlineDataEntry<Vector>) point._2()).getDataEntry();
                    MaxLogger.debug(GenericOnlineDataGenerator_UserUser.class, "[X,y]:" + ", userId " + indexs._1() + ", userId:" + indexs._2()
                                    + ": [" + v.apply(0) + " , " + v.apply(1) + " , " + v.apply(2) + " ] ");
                }
                // debugging ends here
                onlineDataRes = new UnifiedOnlineData(result);
            }
        }

        MaxLogger.info(GenericOnlineDataGenerator_UserUser.class, "---------------- [generateOnlineDataByAppLogs] END ");

        return onlineDataRes;
    }

    public List<Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser>> convertAppLogRecord2Tuple2(AppLogRecord appLogRecord) {

        List<Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser>> res = new ArrayList<Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser>>();

        long userId = appLogRecord.getUserId();
        int actionName = appLogRecord.getActionName();

        if (userId == -1) {
            return null;
        }

        Long userID = new Long(userId);

        if (actionName == AppLogRecord.ACTION_interest) {

            List<Long> otherUserIDs = RedisHelper.getInstance().getAllOtherUsersByInterest(userID);

            if (otherUserIDs != null) {
                for (Long otherUserID : otherUserIDs) {

                    GenericFeatureVector_UserUser lfv = createFeatureVector_UserUser();

                    lfv.setFieldValue(UserUserFeatureField.interest, quantize_interest(appLogRecord), appLogRecord.getTimestamp());

                    Tuple2<Long, Long> userIdAndUserId = new Tuple2<Long, Long>(userID, otherUserID);

                    Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser> tuple2 =
                            new Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser>(userIdAndUserId, lfv);

                    res.add(tuple2);
                }
            }
        }
        else if (actionName == AppLogRecord.ACTION_endorse) {

            long anotherUserId = appLogRecord.getContextValue(AppLogRecord.CONTEXT_NAME_giverId);

            if (anotherUserId != -1) {
                Tuple2<Long, Long> userIdAndUserId = new Tuple2<Long, Long>(userID, anotherUserId);

                GenericFeatureVector_UserUser lfv = createFeatureVector_UserUser();
                lfv.setFieldValue(UserUserFeatureField.endorse, quantize_endorse(appLogRecord), appLogRecord.getTimestamp());

                Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser> tuple2 =
                        new Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser>(userIdAndUserId, lfv);

                res.add(tuple2);

                MaxLogger.debug(GenericOnlineDataGenerator_UserUser.class,
                        "----------------[map2UserIdTuple2 endorse] userId:" + userId + " , anotherUserId:" + anotherUserId + "----------------");
            }
        }
        else if (actionName == AppLogRecord.ACTION_add) {

            long anotherUserId = appLogRecord.getActionId();

            Tuple2<Long, Long> userIdAndUserId = new Tuple2<Long, Long>(userID, anotherUserId);

            GenericFeatureVector_UserUser lfv = createFeatureVector_UserUser();
            lfv.setFieldValue(UserUserFeatureField.addP, quantize_addP(appLogRecord), appLogRecord.getTimestamp());

            Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser> tuple2 =
                    new Tuple2<Tuple2<Long, Long>, GenericFeatureVector_UserUser>(userIdAndUserId, lfv);

            res.add(tuple2);

            MaxLogger.debug(GenericOnlineDataGenerator_UserUser.class,
                    "---------------- [map2UserIdTuple2 add person] userId:" + userId + " , anotherUserId:" + anotherUserId + "----------------");
        }

        return res;
    }

    private GenericFeatureVector_UserUser accumulate(
            GenericFeatureVector_UserUser lfv_1,
            GenericFeatureVector_UserUser lfv_2) {
        return lfv_1.combine(lfv_2);
    }

    private double quantize_endorse(AppLogRecord appLogRecord) {

        int actionValue = appLogRecord.getActionValue();
        if ( actionValue == AppLogRecord.ACTION_VAL_true ) {
            return 1;
        }
        else
            return 0;
    }

    private double quantize_addP(AppLogRecord appLogRecord) {
        return 1;
    }

    private double quantize_interest(AppLogRecord appLogRecord) {

        int actionValue = appLogRecord.getActionValue();
        long interestId = appLogRecord.getActionId();
        if (actionValue == AppLogRecord.ACTION_VAL_add) {
            return 1;
        }
        else if (actionValue == AppLogRecord.ACTION_VAL_remove) {
            return -1;
        }

        return 0;
    }
}
