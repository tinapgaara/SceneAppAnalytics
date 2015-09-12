package maxA.userProfile.feature.userUser;

import maxA.userProfile.IOnlineDataEntry;
import maxA.userProfile.feature.GenericFeature;
import maxA.userProfile.impl.UnifiedOnlineDataEntry;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by TAN on 7/5/2015.
 */
public class UserUserFeature extends GenericFeature {

    public static final String FEATURE_NAME = "UserUser";

    private double mUserUsertMatrixThreshold;

    public double getUserUserThreshold() {
        return mUserUsertMatrixThreshold;
    }

    private JavaPairRDD<Long, Long> mEndorsePairs;

    public UserUserFeature() {

        super(FEATURE_NAME);
        registerField(UserUserFeatureField.endorse);
        registerField(UserUserFeatureField.addP);
        registerField(UserUserFeatureField.interest);

        mEndorsePairs = null;
        mUserUsertMatrixThreshold = 0;
    }

    public JavaPairRDD<Long, Long> getEndorsePairs() {
        return mEndorsePairs;
    }

    public void setEndorsePairs(JavaPairRDD<Long, Long> pairs) {
        mEndorsePairs = pairs;
    }

    public void updateEndorsePairs(JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> dataRDD) {

        if (dataRDD == null) {
            MaxLogger.error(UserUserFeature.class, ErrMsg.ERR_MSG_NULLUserEndorseData);
            return;
        }

        JavaPairRDD<Long, Long> endorsePairsRDD = dataRDD.flatMapToPair(
            new PairFlatMapFunction<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>, Long, Long>() {
                @Override
                public Iterable<Tuple2<Long, Long>> call(Tuple2<Tuple2<Long, Long>, IOnlineDataEntry> tuple2) throws Exception {
                    List<Tuple2<Long, Long>> res = new ArrayList<Tuple2<Long, Long>>();
                    Tuple2<Long, Long> indexIds = tuple2._1();

                    long userId = indexIds._1();
                    long otherUserId = indexIds._2();

                    Vector vector = ( (UnifiedOnlineDataEntry<Vector>) tuple2._2() ).getDataEntry();;
                    double endorseVal = vector.apply(UserUserFeatureField.endorse.getIndex());

                    if (endorseVal != 0) {
                        res.add(new Tuple2<Long, Long>(userId, otherUserId));
                    }
                    return res;
                }
            });

        if (mEndorsePairs != null) {
            endorsePairsRDD = mEndorsePairs.union(endorsePairsRDD);
            endorsePairsRDD = endorsePairsRDD.distinct(); // delete same parts
        }
        mEndorsePairs = endorsePairsRDD;
    }

    public JavaPairRDD<Long, Iterable<Long>> groupEndorsePairsByKey() {

        if (mEndorsePairs == null) {
            MaxLogger.error(UserUserFeature.class, ErrMsg.ERR_MSG_NULLUserEndorseData);
            return null;
        }
        JavaPairRDD<Long, Iterable<Long>> groups = mEndorsePairs.groupByKey();

        return groups;
    }

    public JavaPairRDD<Long, Iterable<Long>> updateGroupsOfEndorsePairs(JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> dataRDD) {

        updateEndorsePairs(dataRDD);
        return groupEndorsePairsByKey();
    }
}
