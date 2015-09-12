package maxA.userProfile.impl.naiveBayes;

import com.google.common.collect.Iterators;

import maxA.common.redis.RedisHelper;
import maxA.userProfile.*;
import maxA.userProfile.attribute.UserInterestSimAttributeValue;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.feature.userUser.UserUserFeature;
import maxA.userProfile.feature.userUser.UserUserFeatureField;
import maxA.userProfile.impl.GenericUserProfileModel;
import maxA.userProfile.impl.UnifiedOnlineDataEntry;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.util.*;

/**
 * Created by max2 on 7/22/15.
 */
public class NB_UserProfileModel extends GenericUserProfileModel {

    private static NB_UserProfileModel m_instance = null;

    private static double TRAIN_USER_PROFILE_MODEL_NaiveBayesModel_lambda = 2.0;

    public static NB_UserProfileModel getInstance() {

        if (m_instance == null) {
            m_instance = new NB_UserProfileModel();
        }
        return m_instance;
    }

    private NB_UserProfileModel() {

        mFeatureModels = null;
        mFeatureMatrixs = null;
    }

    @Override
    protected IUserProfile createUserProfile(Long userID) { //factory method
        // debugging starts here
        MaxLogger.debug(NB_UserProfileModel.class,
                        "------------------ [createUserProfile] -----------------");
        // debugging ends here

        return new NB_UserProfile(userID, this);
    }

    @Override
    // factory method
    protected Object doTrainFeatureModel(IFeature feature, ITrainData trainData) {

        String featureName = feature.getName();
        NaiveBayesModel nbModel = null;

        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {
            // Train the model using LinearRegression
            JavaRDD<LabeledPoint> data = trainData.getData();
            if (data == null) {
                MaxLogger.error(NB_UserProfileModel.class,
                                ErrMsg.ERR_MSG_NullTrainData);
                return null;
            }

            nbModel = NaiveBayes.train(
                    data.rdd(), TRAIN_USER_PROFILE_MODEL_NaiveBayesModel_lambda);

            double[] NaiveLabels = nbModel.labels(); // 0 or 1, classify to two categories
            double[] pi = nbModel.pi();
            double[][] theta = nbModel.theta();

            // debugging starts here
            /*
            MaxLogger.debug(NB_UserProfileModel.class,
                            "------------------ [trainFeatureModel]: [NB: pi: " + Math.exp(pi[0]) + ", " +
                                                Math.exp(pi[1]) + "] ------------------");
            */
            // debugging ends here

            for (int i = 0 ; i < theta.length; i ++) {
                MaxLogger.debug(NB_UserProfileModel.class,
                        "------------------ [trainFeatureModel]: [NB: theta for " + i + "th class ]:");
                double[] weights = theta[i];
                for (int j = 0 ; j < weights.length; j ++) {
                    MaxLogger.debug(NB_UserProfileModel.class, Math.exp(weights[j]) + ",");
                }
                MaxLogger.debug(NB_UserProfileModel.class, " ------------------ ");

            }
        }
        else if ( featureName.equals(UserUserFeature.FEATURE_NAME ) ) {

            // TODO: change the model to be trained in future

            int lenLabels = 2;
            int lenWeights = 3;
            // labels list of labels
            double[] labels = new double[]{0,1};
            // pi log of class priors, whose dimension is C, number of labels
            double[] pi = new double[]{Math.log(0.5), Math.log(0.5)};
            //  theta log of class conditional probabilities, whose dimension is C-by-D, where D is number of features
            double[][] theta = new double[lenLabels][lenWeights];

            double[] weights = new double[]{Math.log(0.5), Math.log(0.2), Math.log(0.3)};

            for (int i = 0 ; i < lenLabels ; i ++) {
                for (int j = 0 ; j < lenWeights ; j ++) {
                    theta[i][j] = weights[j];
                }
            }
            nbModel = new NaiveBayesModel(labels, pi, theta);
        }
        else {
            MaxLogger.error(NB_UserProfileModel.class,
                            ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, featureName));
        }

        return nbModel;
    }

    @Override
    protected void updateByOnlineDataAndFeatureModel(IFeature feature, IOnlineData onlineData, Object featureModel) {

        final NaiveBayesModel nbModel = (org.apache.spark.mllib.classification.NaiveBayesModel) featureModel;
        if (nbModel == null) {
            MaxLogger.error(NB_UserProfileModel.class,
                            ErrMsg.ERR_MSG_NullUserProfileModel);
            return;
        }

        String featureName = feature.getName();

        JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> dataRDD = onlineData.getData();
        JavaRDD<MatrixEntry> matrixEntriesRDD = null;

        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {

            final double threshold = ((UserMerchantFeature) feature).getUserMerchantThreshold();

            matrixEntriesRDD = dataRDD.map
                    (new org.apache.spark.api.java.function.Function<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>, MatrixEntry>() {
                        public MatrixEntry call(Tuple2<Tuple2<Long, Long>, IOnlineDataEntry> tuple2) throws Exception {

                            Tuple2<Long, Long> indexIds = tuple2._1();
                            long userId = indexIds._1();
                            long merchantId = indexIds._2();

                            LabeledPoint point = ((UnifiedOnlineDataEntry<LabeledPoint>) tuple2._2()).getDataEntry();
                            Vector vector = point.features();
                            double label = point.label();

                            double classfication = nbModel.predict(vector);

                            return new MatrixEntry(userId, merchantId, classfication);
                        }
                    });
        } else if (featureName.equals(UserUserFeature.FEATURE_NAME)) {

            final double threshold = ((UserUserFeature) feature).getUserUserThreshold();

            UserUserFeature usrUserFeature = (UserUserFeature) (feature);
            JavaPairRDD<Long, Iterable<Long>> groups = usrUserFeature.updateGroupsOfEndorsePairs(dataRDD);
            final Map<Long, Map<Long, Double>> resMap = mapEndorseGroups(groups);

            matrixEntriesRDD = dataRDD.map(new org.apache.spark.api.java.function.Function<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>, MatrixEntry>() {
                public MatrixEntry call(Tuple2<Tuple2<Long, Long>, IOnlineDataEntry> tuple2) throws Exception {

                    Tuple2<Long, Long> indexIds = tuple2._1();
                    long userId = indexIds._1();
                    long otherUserId = indexIds._2();

                    Vector vector = ((UnifiedOnlineDataEntry<Vector>) tuple2._2()).getDataEntry();

                    double interestSim = getOrUpdateInterestSim(vector, userId, otherUserId);
                    double endorseVal = updateEndorseValue(vector, resMap, userId, otherUserId);

                    int[] indexs = new int[]{0,1,2};
                    double[] values = new double[]{endorseVal,vector.apply(1),interestSim};
                    Vector newVector = Vectors.sparse(indexs.length, indexs, values);

                    double classfication = nbModel.predict(newVector);

                    return new MatrixEntry(userId, otherUserId, classfication);
                }
            });
        } else {
            MaxLogger.error(NB_UserProfileModel.class, ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, featureName));
            return;
        }

        if (matrixEntriesRDD == null) {
            MaxLogger.info(NB_UserProfileModel.class,
                    "----------------[updateByOnlineDataAndFeatureModel] RESULT-length = NULL " + "----------------");
        } else {
            // debugging starts here
            List<MatrixEntry> entris = matrixEntriesRDD.collect();
            if (entris.size() > 0) {
                MaxLogger.info(NB_UserProfileModel.class,
                        "----------------[updateByOnlineDataAndFeatureModel] RESULT-length = [" + entris.size() + "]" + "----------------");
            }

            for (MatrixEntry entry : entris) {
                long userId = entry.i();
                long merchantId = entry.j();
                MaxLogger.debug(NB_UserProfileModel.class, featureName + "[i,j]:" + ", rowId " + userId + ", colId:" + merchantId
                        + ", classification:" + entry.value());
            }

            if (featureName.equals(UserUserFeature.FEATURE_NAME)) {
                Set<Map.Entry<Long, Map<String, UserAttributeValue>>> entries = mUserAttributes.entrySet();
                for (Map.Entry<Long, Map<String, UserAttributeValue>> entry : entries) {
                    System.out.println("****************** userId:" + entry.getKey());
                    Map<String, UserAttributeValue> values = entry.getValue();
                    Set<Map.Entry<String, UserAttributeValue>> ls = values.entrySet();
                    for (Map.Entry<String, UserAttributeValue> value : ls) {
                        UserInterestSimAttributeValue attriValue = (UserInterestSimAttributeValue) value.getValue();
                        Map<Long, Double> others = attriValue.getUserInterestSim();
                        Set<Map.Entry<Long, Double>> ens = others.entrySet();
                        for (Map.Entry<Long, Double> en : ens) {
                            Long oUserId = en.getKey();
                            double sim = en.getValue();
                            System.out.println("****************** otherUserId:" + oUserId + ", sim:" + sim);
                        }
                    }
                }
            }
            // debugging ends here

            updateByOnlineData(feature, matrixEntriesRDD);
        }
    }

    /*
    @Override
    protected void updateByOnlineDataAndFeatureModel(int featureId, IOnlineData onlineData, Object featureModel) {

       IFeature feature = getFeature(featureId);

        final NaiveBayesModel nbModel = (org.apache.spark.mllib.classification.NaiveBayesModel) featureModel;
        if (nbModel == null) {
            MaxLogger.error(NB_UserProfileModel.class,
                            ErrMsg.ERR_MSG_NullUserProfileModel);
            return;
        }

        String featureName = feature.getName();

        JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> dataRDD = onlineData.getData();
        JavaRDD<MatrixEntry> matrixEntriesRDD = null;

        if (featureName.equals(UserMerchantFeature.FEATURE_NAME)) {

            final double threshold = ((UserMerchantFeature) feature).getUserMerchantThreshold();

            matrixEntriesRDD = dataRDD.map
                    (new org.apache.spark.api.java.function.Function<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>, MatrixEntry>() {
                        public MatrixEntry call(Tuple2<Tuple2<Long, Long>, IOnlineDataEntry> tuple2) throws Exception {

                            Tuple2<Long, Long> indexIds = tuple2._1();
                            long userId = indexIds._1();
                            long merchantId = indexIds._2();

                            LabeledPoint point = ((UnifiedOnlineDataEntry<LabeledPoint>) tuple2._2()).getDataEntry();
                            Vector vector = point.features();
                            double label = point.label();

                            double classfication = nbModel.predict(vector);

                            return new MatrixEntry(userId, merchantId, classfication);
                        }
                    });
        } else if (featureName.equals(UserUserFeature.FEATURE_NAME)) {

            final double threshold = ((UserUserFeature) feature).getUserUserThreshold();

            UserUserFeature usrUserFeature = (UserUserFeature) (mFeatures.get(featureId));
            JavaPairRDD<Long, Iterable<Long>> groups = usrUserFeature.updateGroupsOfEndorsePairs(dataRDD);
            final Map<Long, Map<Long, Double>> resMap = mapEndorseGroups(groups);

            matrixEntriesRDD = dataRDD.map(new org.apache.spark.api.java.function.Function<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>, MatrixEntry>() {
                public MatrixEntry call(Tuple2<Tuple2<Long, Long>, IOnlineDataEntry> tuple2) throws Exception {

                    Tuple2<Long, Long> indexIds = tuple2._1();
                    long userId = indexIds._1();
                    long otherUserId = indexIds._2();

                    Vector vector = ((UnifiedOnlineDataEntry<Vector>) tuple2._2()).getDataEntry();

                    double interestSim = getOrUpdateInterestSim(vector, userId, otherUserId);
                    double endorseVal = updateEndorseValue(vector, resMap, userId, otherUserId);

                    int[] indexs = new int[]{0,1,2};
                    double[] values = new double[]{endorseVal,vector.apply(1),interestSim};
                    Vector newVector = Vectors.sparse(indexs.length, indexs, values);

                    double classfication = nbModel.predict(newVector);

                    return new MatrixEntry(userId, otherUserId, classfication);
                }
            });
        } else {
            MaxLogger.error(NB_UserProfileModel.class, ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature, featureName));
            return;
        }

        if (matrixEntriesRDD == null) {
            MaxLogger.info(NB_UserProfileModel.class,
                            "----------------[updateByOnlineDataAndFeatureModel] RESULT-length = NULL " + "----------------");
        } else {
            // For debug information
            List<MatrixEntry> entris = matrixEntriesRDD.collect();
            if (entris.size() > 0) {
                MaxLogger.info(NB_UserProfileModel.class,
                                "----------------[updateByOnlineDataAndFeatureModel] RESULT-length = [" + entris.size() + "]" + "----------------");
            }

            for (MatrixEntry entry : entris) {
                long userId = entry.i();
                long merchantId = entry.j();
                MaxLogger.debug(NB_UserProfileModel.class, featureName + "[i,j]:" + ", rowId " + userId + ", colId:" + merchantId
                                + ", classification:" + entry.value());
            }

            if (featureName.equals(UserUserFeature.FEATURE_NAME)) {
                Set<Map.Entry<Long, Map<String, UserAttributeValue>>> entries = mUserAttributes.entrySet();
                for (Map.Entry<Long, Map<String, UserAttributeValue>> entry : entries) {
                    System.out.println("****************** userId:" + entry.getKey());
                    Map<String, UserAttributeValue> values = entry.getValue();
                    Set<Map.Entry<String, UserAttributeValue>> ls = values.entrySet();
                    for (Map.Entry<String, UserAttributeValue> value : ls) {
                        UserInterestSimAttributeValue attriValue = (UserInterestSimAttributeValue) value.getValue();
                        Map<Long, Double> others = attriValue.getUserInterestSim();
                        Set<Map.Entry<Long, Double>> ens = others.entrySet();
                        for (Map.Entry<Long, Double> en : ens) {
                            Long oUserId = en.getKey();
                            double sim = en.getValue();
                            System.out.println("****************** otherUserId:" + oUserId + ", sim:" + sim);
                        }
                    }
                }
            }
            //

            updateByOnlineData(featureId, featureName, matrixEntriesRDD);
        }
    }
    //*/

     public Map<Long, Map<Long, Double>> mapEndorseGroups(JavaPairRDD<Long, Iterable<Long>> groups) {

        if (groups == null) {
            MaxLogger.error(NB_UserProfileModel.class, ErrMsg.ERR_MSG_NULLUserEndorseData);
            return null;
        }

        final Map<Long, Map<Long, Double>> resMap = new HashMap<Long, Map<Long, Double>>();

        groups.foreach(new VoidFunction<Tuple2<Long, Iterable<Long>>>() {
            public void call(Tuple2<Long, Iterable<Long>> tuple2) throws Exception {
                long key = tuple2._1();
                Iterable<Long> values = tuple2._2();
                int size = Iterators.size(values.iterator());

                Iterator<Long> iterator = values.iterator();
                Map<Long, Double> map = new HashMap<Long, Double>();
                while (iterator.hasNext()) {
                    long pairUserId = iterator.next();
                    double numerator = 1;
                    double quantVal = numerator / size;
                    map.put(pairUserId, quantVal);
                }
                resMap.put(key, map);
            }
        });

        return resMap;
    }

    public double getOrUpdateInterestSim(Vector quantVector, long userId, long otherUserId) {

        double interestQuant = quantVector.apply(UserUserFeatureField.interest.getIndex());

        double interestSim = 0;

        RedisHelper redisHelper = RedisHelper.getInstance();

        UserInterestSimAttributeValue attributeValue = (UserInterestSimAttributeValue)
                getUserAttributeValue(userId, UserAttribute.interestSimAttibuteName);

        if ( (attributeValue == null) ||
                (interestQuant != 0 ) || ( ! attributeValue.ifContainsSimWithOtherUser(otherUserId) ) ) {

            Vector v_1 = redisHelper.getUserInterestVector(userId);
            Vector v_2 = redisHelper.getUserInterestVector(otherUserId);

            interestSim = calDistance(v_1, v_2);

            if (attributeValue == null) {
                // debugging starts here
                MaxLogger.debug(NB_UserProfileModel.class,
                                "-------------- attribute value is null. userId:" + userId + ", otherUserId:"+otherUserId);
                // debugging ends here
            }

            if (interestSim != 0 ) {

                if (attributeValue == null) {

                    attributeValue = new UserInterestSimAttributeValue();
                    attributeValue.addInterestSimWithOtherUser(otherUserId, interestSim);
                    addUserAttributeValue(userId, UserAttribute.interestSimAttibuteName, attributeValue);
                }
                else {

                    attributeValue.addInterestSimWithOtherUser(otherUserId, interestSim);

                }
            }
        }
        else {
            interestSim = attributeValue.getSimWithOtherUser(otherUserId);

            // debugging starts here
            MaxLogger.debug(NB_UserProfileModel.class,
                            "-------------- get previous interest val " + otherUserId + " userId, " +
                            "old distance:" + interestSim);
            // debugging ends here
        }

        return interestSim;
    }

    public double updateEndorseValue(Vector vector, Map<Long, Map<Long, Double>> endorseMap, long userId, long otherUserId) {

        double endorseQuant = vector.apply(UserUserFeatureField.endorse.getIndex());
        double endorseVal = 0;

        if ( (endorseQuant != 0) && (endorseMap != null) ) {
            Map<Long, Double> userMap = endorseMap.get(userId);
            if (userMap != null) {
                Double value = userMap.get(otherUserId);
                if (value != null) {
                    endorseVal = value.doubleValue();
                }
            }
        }
        return endorseVal;
    }

    public double calDistance(Vector v_1, Vector v_2) {

        if ( (v_1 == null) || (v_2 == null) ) {
            MaxLogger.info(NB_UserProfileModel.class,
                            "----------------[calDistance] One of these user has not choose his interest restaurants. ");

            return 0;
        }

        return Vectors.sqdist(v_1, v_2);
    }
}
