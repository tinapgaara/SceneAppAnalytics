package maxA.recommender.ALS.impl;

import maxA.recommender.IRecommenderData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.Rating;

/**
 * Created by max2 on 8/4/15.
 */
public class ALS_RecommenderData implements IRecommenderData<Rating> {

    private JavaRDD<Rating> mRecommenderData;

    public ALS_RecommenderData() { mRecommenderData = null; }

    public ALS_RecommenderData(JavaRDD<Rating> recommenderData) { mRecommenderData = recommenderData; }

    @Override
    public void setData(JavaRDD<Rating> recommenderData) { mRecommenderData = recommenderData; }

    @Override
    public JavaRDD<Rating> getData() { return mRecommenderData; }

    @Override
    public void release() {

        if (mRecommenderData != null) {
            mRecommenderData = null;
        }
    }
}
