package maxA.userProfile.impl;

import maxA.userProfile.*;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by TAN on 7/5/2015.
 */
public abstract class GenericUserProfile implements IUserProfile , Serializable {

    protected Long mUserID;

    private GenericUserProfileModel mModel;

    protected GenericUserProfile(Long userID, GenericUserProfileModel model) {

        mUserID = userID;
        mModel = model;
    }

    @Override
    public int getUserId() {
        return mUserID.intValue();
    }

    @Override
    public UserAttributeValue getUserAttributeValue(String attributeName) {

        return mModel.getUserAttributeValue(mUserID, attributeName);
    }

    @Override
    public List<Tuple2<Double,Long>> getTopItems(String featureName, int maxNumOfItems) {

        if (mModel == null) {
            MaxLogger.error(GenericUserProfile.class,
                    ErrMsg.ERR_MSG_NullUserProfileModel);
            return null;
        }

        CoordinateMatrix coordinateMatrix = mModel.getFeatureMatrix(featureName);
        if (coordinateMatrix == null) {
            MaxLogger.error(GenericUserProfile.class,
                    ErrMsg.ERR_MSG_NullMatrix);
        }

        IndexedRowMatrix indexedRowMatrix = coordinateMatrix.toIndexedRowMatrix();
        JavaPairRDD<Double, Long> pairs = indexedRowMatrix.rows().toJavaRDD().flatMapToPair(
            new PairFlatMapFunction<IndexedRow, Double, Long>() {
                @Override
                public Iterable<Tuple2<Double, Long>> call (IndexedRow row) {
                    List<Tuple2<Double, Long>> res = new ArrayList<Tuple2<Double, Long>>();
                    long userId = row.index();
                    if (userId == mUserID.longValue()) {
                        Vector rowVector = row.vector(); // get all value from 12- 71
                        for (int i = 0; i < rowVector.size(); i++) {
                            Double val = new Double(rowVector.apply(i));
                            Long colID = new Long(i);
                            if (val > 0) {
                                res.add(new Tuple2<Double, Long>(val, colID));
                            }
                        }
                    }
                    return res;
                }
            });

        JavaPairRDD<Double, Long> sortedPairs = pairs.sortByKey(false);
        List<Tuple2<Double,Long>> sortedCols = sortedPairs.collect();
        return sortedCols;
    }

    @Override
    public void release() {

        mModel = null;
    }

}
