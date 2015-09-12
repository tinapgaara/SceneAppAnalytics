package maxA.recommender.ALS.impl;

import jdk.nashorn.internal.ir.Assignment;
import maxA.cluster.IClusterResult;
import maxA.cluster.IUserClusterModel;
import maxA.common.Constants;
import maxA.recommender.IMerchantRecommendation;
import maxA.recommender.IRecommender;
import maxA.recommender.IRecommenderData;
import maxA.recommender.impl.MerchantRecommendation;
import maxA.userProfile.IUserProfileModel;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by max2 on 7/29/15.
 */
public class ALS_Recommender implements IRecommender, Serializable {

    private IUserClusterModel mUserClusterModel;

    private IUserProfileModel mUserProfileModel;

    private static ALS_Recommender m_instance = null;

    public ALS_Recommender(IUserClusterModel userClusterModel, IUserProfileModel userProfileModel) {

        mUserClusterModel = userClusterModel;
        mUserProfileModel = userProfileModel;
    }

    public static ALS_Recommender getInstance(IUserClusterModel userClusterModel, IUserProfileModel userProfileModel) {

        if (m_instance == null) {
            m_instance = new ALS_Recommender(userClusterModel, userProfileModel);
        }
        return m_instance;
    }

    @Override
    public void setUserClusterModel(IUserClusterModel userClusterModel) {

        mUserClusterModel = userClusterModel;
    }

    @Override
    public IUserClusterModel getUserClusterModel() {

        return mUserClusterModel;
    }

    @Override
    public void setUserProfileModel(IUserProfileModel userProfileModel) {

        mUserProfileModel = userProfileModel;
    }

    @Override
    public IUserProfileModel getUserProfileModel() {

        return mUserProfileModel;
    }

    public IRecommenderData getDataFromUserProfileModel() {

        // debugging starts here
        MaxLogger.debug(ALS_Recommender.class, "----------------[getDataFromUserProfileModel] BEGIN ...");
        // debugging ends here

        if (mUserProfileModel == null) {
            return null;
        }
        /*
        int featureId = mUserProfileModel.getFeatureIdByName(UserMerchantFeature.FEATURE_NAME);
        if (featureId == 0) {
            MaxLogger.error(ALS_Recommender.class,
                            ErrMsg.ERR_MSG_UnregisteredFeature);
        }
        //*/

        CoordinateMatrix userMerchantMatrix = mUserProfileModel.getFeatureMatrix(UserMerchantFeature.FEATURE_NAME);
        if (userMerchantMatrix == null) {
            MaxLogger.error(ALS_Recommender.class,
                            ErrMsg.ERR_MSG_NullMatrix);
            return null;
        }

        JavaRDD<Rating> recommenderData = userMerchantMatrix.entries().toJavaRDD().map(
                new Function<MatrixEntry, Rating>() {
                    @Override
                    public Rating call(MatrixEntry entry) {
                        int user = (int) entry.i();
                        int product = (int) entry.j();
                        double value = entry.value();

                        return new Rating(user, product, value);
                    }
                });

        IRecommenderData resData = null;

        // debugging starts here
        if (recommenderData == null) {
            MaxLogger.debug(ALS_Recommender.class,
                    "----------------[getDataFromUserProfileModel] RESULT-length = NULL " + "----------------");
        }
        else {
            List<Rating> ratingList = recommenderData.collect();
            int count = ratingList.size();
            MaxLogger.debug(ALS_Recommender.class,
                    "----------------[getDataFromUserProfileModel] RESULT-length = [" + count + "]" + "----------------");

            if (count > 0) {
                for (int i = 0; i < count; i ++ ) {
                    Rating rating = ratingList.get(i);
                    int user = rating.user();
                    int merchant = rating.product();
                    double value = rating.rating();

                    MaxLogger.debug(ALS_Recommender.class,
                            "----------------[getDataFromUserProfileModel] user:" + user + " , merchant:" + merchant + ", rating:" + value);
                }
                resData = new ALS_RecommenderData(recommenderData);
            }
        }

        MaxLogger.debug(ALS_Recommender.class, "---------------- [getDataFromUserProfileModel] END ");
        // debugging ends here

        return resData;
    }

    public List<IMerchantRecommendation> recMerchants2User(long userId, final int maxNumOfMerchants) {

        // 1. After implementing ALS on Vum, recommend merchants for this user

        // get Vum from userProfileModel, and map it to JavaRDD<Rating>
        IRecommenderData umData = getDataFromUserProfileModel();

        if (umData == null) {

            MaxLogger.error(ALS_Recommender.class, ErrMsg.ERR_MSG_NullMatrix);
            return null;
        }

        JavaRDD<Rating> ratingsRDD = umData.getData();
        if (ratingsRDD == null) {

            MaxLogger.error(ALS_Recommender.class, ErrMsg.ERR_MSG_NullRecommenderData);
            return null;
        }

        final MatrixFactorizationModel ALSModel = ALS.train(ratingsRDD.rdd(), Constants.ALS_RECOMMENDER_MODEL_RANK,
                Constants.ALS_RECOMMENDER_MODEL_NumIterations, Constants.ALS_RECOMMENDER_MODEL_LAMBDA);

        Rating[] ratings = null;
        try {
            ratings = ALSModel.recommendProducts((int) userId, maxNumOfMerchants); // in rank
        } catch (java.util.NoSuchElementException e) {
            MaxLogger.info(ALS_Recommender.class,
                            "No suitable recommendation merchants ! ");
            return null;
        }

        // debugging starts here
        for(int i = 0; i < ratings.length; i ++) {
            MaxLogger.debug(ALS_Recommender.class,
                            "---------------- [recMerchants2User]: "+ i +"th merchant's ID: " + ratings[i].product());
        }
        // debugging ends here
        List<IMerchantRecommendation> recommendedMerchantRatings = mapRating2MerchantRecommendation(ratings);


        // 2. Based on the clusters, get similar users
        IClusterResult result = mUserClusterModel.filterClusterByUserId(userId);
        if (result == null) {
            MaxLogger.error(ALS_Recommender.class, ErrMsg.ERR_MSG_NullClusterData);
            return null;
        }
        JavaRDD<PowerIterationClustering.Assignment> simUsers = result.getResult();
        if (simUsers == null) {
            MaxLogger.error(ALS_Recommender.class, ErrMsg.ERR_MSG_NullClusterData);
            return null;
        }

        List<PowerIterationClustering.Assignment> simUsersList = simUsers.collect();

        // 3. recommend merchants to similar users.
        //    and combine these merchants to this user's recommendations.
        for (PowerIterationClustering.Assignment assignment : simUsersList) {

            try {
                Rating[] curRatings = ALSModel.recommendProducts((int) (assignment.id()), maxNumOfMerchants);
                List<IMerchantRecommendation> curRecommendations = mapRating2MerchantRecommendation(curRatings);
                recommendedMerchantRatings.addAll(curRecommendations);

            } catch(NoSuchElementException e) {
                MaxLogger.info(ALS_Recommender.class,
                                "---------------- [recMerchants2User]: No recommendations for similar users ----------------");
                break;
            }
        }

        // debugging starts here
        for(int i = 0; i < recommendedMerchantRatings.size(); i ++) {
            MaxLogger.debug(ALS_Recommender.class,
                            "---------------- [recMerchants2User combine clusters]: merchantID: "+ recommendedMerchantRatings.get(i).getMerchantId()
                            +", rating:" + recommendedMerchantRatings.get(i).getLikeDegree());
        }
        // debugging ends here

        return reorder(recommendedMerchantRatings, maxNumOfMerchants);
    }

    public List<IMerchantRecommendation> reorder(List<IMerchantRecommendation> recommendedMerchantRatings, int maxNumOfMerchants) {

        if (recommendedMerchantRatings == null) {
            MaxLogger.error(ALS_Recommender.class, ErrMsg.ERR_MSG_NullRecommenderData);
        }

        Collections.sort(recommendedMerchantRatings, new MerchantRecommendation.MerchantRecommendationComparators());
        List<IMerchantRecommendation> res = recommendedMerchantRatings.subList(0, maxNumOfMerchants - 1);

        for(int i = 0; i < recommendedMerchantRatings.size(); i ++) {

            MaxLogger.info(ALS_Recommender.class,
                            "---------------- [recMerchants2User reorder]: top " + i +"th recommendation: " +
                            "[ merchantID:" + recommendedMerchantRatings.get(i).getMerchantId() +", " +
                            "rating:"+ recommendedMerchantRatings.get(i).getLikeDegree() + "]");
        }

        return res;
    }

    public List<IMerchantRecommendation> mapRating2MerchantRecommendation(Rating[] ratingArr) {

        List<IMerchantRecommendation> recommendedMerchantRatings = new ArrayList<IMerchantRecommendation>();
        for (int i = 0; i < ratingArr.length; i ++) {

            Rating rating = ratingArr[i];
            int user = rating.user();
            int merchant = rating.product();
            double value = rating.rating();

            MerchantRecommendation merchantRecommendation = new MerchantRecommendation((long)merchant, value);
            recommendedMerchantRatings.add(merchantRecommendation);
        }
        return recommendedMerchantRatings;
    }
}
