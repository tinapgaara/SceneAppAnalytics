package maxA.userProfile.impl;

import maxA.userProfile.*;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

/**
 * Created by TAN on 7/5/2015.
 */
public abstract class GenericUserProfileModel implements IUserProfileModel, Serializable {

    protected static Map<Long, Map<String, UserAttributeValue>> mUserAttributes;
    protected Map<Long, IUserProfile> mUserProfiles;

    protected Set<IFeature> mFeatures;
    /*
    protected Map<Integer, IFeature> mFeatures;
    protected int mNextFeatureId;

    protected Map<Integer, FeatureModelWrapper> mFeatureModels;
    protected Map<Integer, CoordinateMatrix> mFeatureMatrixs;
    //*/
    protected Map<String, FeatureModelWrapper> mFeatureModels;
    protected Map<String, CoordinateMatrix> mFeatureMatrixs;

    protected abstract IUserProfile createUserProfile(Long userID);
    protected abstract Object doTrainFeatureModel(IFeature feature, ITrainData trainData);
    /*
    protected abstract void updateByOnlineDataAndFeatureModel(int featureId, IOnlineData testData, Object featureModel);
    //*/
    protected abstract void updateByOnlineDataAndFeatureModel(IFeature feature, IOnlineData testData, Object featureModel);

    protected GenericUserProfileModel() {

        mUserAttributes = null;
        mUserProfiles = null;
        
        mFeatures = null;
        /*
        mNextFeatureId = 1;
        //*/

        mFeatureModels = null;
        mFeatureMatrixs = null;
    }

    @Override
    public void addUserAttributeValue(long userId, String attributeName, UserAttributeValue attribute) {

        if (attribute == null) {
            return;
        }

        if ( (attributeName == null) || attributeName.isEmpty() ) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_IllegalUserAttributeName,
                            (attributeName == null) ? "NULL" : "Empty"));
        }

        Map<String, UserAttributeValue> existMap = null;
        UserAttributeValue existAttribute = null;
        if (mUserAttributes == null) {
            mUserAttributes = new HashMap<Long, Map<String, UserAttributeValue>>();
        }
        else {
            existMap = mUserAttributes.get(userId);
            if (existMap != null) {
                existAttribute = existMap.get(attributeName);
            }
        }

        if (existMap == null) {
            Map<String, UserAttributeValue> newMap = new HashMap<String, UserAttributeValue>();

            newMap.put(attributeName, attribute);
            mUserAttributes.put(userId, newMap);
        }
        else {
            if (existAttribute != null) {
                existAttribute.release();
                existAttribute = null;
            }
            existMap.put(attributeName, attribute);
        }
    }

    @Override
    public UserAttributeValue getUserAttributeValue(long userId, String attributeName) {
        if (mUserAttributes == null) {

            // debugging starts here
            MaxLogger.debug(GenericUserProfileModel.class,
                            "-------------- mUserAttributes == null");
            // debugging ends here
            return null;
        }

        Map<String, UserAttributeValue> map = mUserAttributes.get(userId);
        if (map == null) {
            return null;
        }

        return map.get(attributeName);
    }

    @Override
    public IUserProfile getUserProfile(long userId) {

        Long userID = new Long(userId);

        IUserProfile userProfile = null;
        if (mUserProfiles == null) {
            mUserProfiles = new HashMap<Long, IUserProfile>();
        }
        else {
            userProfile = mUserProfiles.get(userID);
        }

        if (userProfile == null) {
            userProfile = createUserProfile(userID);
            mUserProfiles.put(userID, userProfile);
        }

        return userProfile;
    }

    @Override
    public void registerFeature(IFeature feature) {

        if (feature == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_NullFeatures);
        }

        if (mFeatures == null) {
            mFeatures = new HashSet<IFeature>();
        }
        mFeatures.add(feature);
    }

    /*
    @Override
    public int registerFeature(IFeature feature) {
        
        if (feature == null) {
            return 0;
        }

        int featureId = 0;

        if (mFeatures == null) {
            mFeatures = new HashMap<Integer, IFeature>();
        }
        else {
            for (Integer key : mFeatures.keySet()) {
                if (mFeatures.get(key).getName().equals(feature.getName())) {
                    featureId = key.intValue();
                    break;
                }
            }
        }

        if (featureId == 0) {
            featureId = mNextFeatureId;
            mFeatures.put(new Integer(featureId), feature);
            mNextFeatureId++;
        }

        return featureId;
    }
    //*/

    @Override
    public void unregisterFeature(IFeature feature) {

        if (mFeatures != null) {
            if (mFeatures.contains(feature)) {
                boolean removeFlag = mFeatures.remove(feature);
                if (! removeFlag) {
                    MaxLogger.info(GenericUserProfileModel.class,
                                   ErrMsg.ERR_MSG_UnregisterFeatureFailed);
                }
            }
        }
    }
    /*
    @Override
    public void unregisterFeature(int featureId) {

        if (mFeatures != null) {
            IFeature feature = mFeatures.remove(new Integer(featureId));
            if (feature != null) {
                feature.release();
                feature = null;
            }
        }
    }
    //*/

    public IFeature getFeature(String featureName) {

        if (mFeatures == null) {
            MaxLogger.info(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_NullFeatures);
        }
        for (IFeature feature : mFeatures) {
            if (feature.getName().equals(featureName)) {
                return feature;
            }
        }

        MaxLogger.info(GenericUserProfileModel.class,
                ErrMsg.ERR_MSG_UnknownFeatureName);
        return null;
    }

    /*
    public IFeature getFeature(int featureId) {

        if (mFeatures == null) {
            return null;
        }

        return mFeatures.get(new Integer(featureId));
    }
    //*/

    @Override
    public Set<IFeature> getAllFeatures() {

        if (mFeatures == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_NullFeatures);
            return null;
        }
        return mFeatures;
    }
    /*
    @Override
    public IFeature[] getAllFeatures() {

        if (mFeatures == null) {
            MaxLogger.error(GenericUserProfileModel.class, ErrMsg.ERR_MSG_NullFeatures);
            return null;
        }

        Collection<IFeature> features = mFeatures.values();
        if ( (features == null) || features.isEmpty() ) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature,
                            (features == null) ? "NULL" : "Empty"));
            return null;
        }

        IFeature[] featureArr = new IFeature[features.size()];
        features.toArray(featureArr);

        return featureArr;
    }
    //*/

    /*
    @Override
    public int getFeatureIdByName(String featureName) {

        if ( (featureName == null) || (mFeatures == null) ) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_UnknownFeature,
                            (featureName == null) ? "featureName NULL" : "mFeatures NULL"));
            return 0;
        }

        int featureId = 0;
        for (Integer featureID : mFeatures.keySet()) {
            IFeature feature = mFeatures.get(featureID);
            if (featureName.equals(feature.getName())) {
                featureId = featureID.intValue();
                break;
            }
        }

        return featureId;
    }
    //*/

    private void setFeatureModel(String featureName, FeatureModelWrapper wrapper) {

        if (wrapper == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_NullUserProfileModel);
            return;
        }

        FeatureModelWrapper oldWrapper = null;
        if (mFeatureModels == null) {
            mFeatureModels = new HashMap<String, FeatureModelWrapper>();
        }
        else {
            oldWrapper = mFeatureModels.get(featureName);
            if (oldWrapper != null) {
                oldWrapper.release();
            }
        }

        mFeatureModels.put(featureName, wrapper);
    }
    /*
    private void setFeatureModel(int featureId, FeatureModelWrapper wrapper) {

        if (wrapper == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_NullUserProfileModel);
            return;
        }

        Integer featureID = new Integer(featureId);

        FeatureModelWrapper oldWrapper = null;
        if (mFeatureModels == null) {
            mFeatureModels = new HashMap<Integer, FeatureModelWrapper>();
        }
        else {
            oldWrapper = mFeatureModels.get(featureID);
            if (oldWrapper != null) {
                oldWrapper.release();
            }
        }

        mFeatureModels.put(featureID, wrapper);
    }
    //*/

    @Override
    public void trainFeatureModel(IFeature feature, ITrainData trainData) {

        if (feature == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_UnknownFeatureId );
            return;
        }

        // TODO: in future, if we want to train User-User model, we should check if trainData is null here rather than in doTrainFeatureModel.
        Object featureModel = doTrainFeatureModel(feature, trainData);
        // setFeatureModel(featureId, new FeatureModelWrapper(featureModel));
        setFeatureModel(feature.getName(), new FeatureModelWrapper(featureModel));
    }
    /*
    @Override
    public void trainFeatureModel(int featureId, ITrainData trainData)
            throws UserProfileException {

        IFeature feature = getFeature(featureId);
        if (feature == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_UnknownFeatureId );
            return;
        }

        // TODO: in future, if we want to train User-User model, we should check if trainData is null here rather than in doTrainFeatureModel.
        Object featureModel = doTrainFeatureModel(feature, trainData);
        setFeatureModel(featureId, new FeatureModelWrapper(featureModel));
    }
    //*/


    public void updateModelByOnlineData(IFeature feature, IOnlineData onlineData) {

        if (feature == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_UnknownFeatureId);
            return;
        }

        if (onlineData == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_NullOnlineData);
            return;
        }

        if (mFeatureModels == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_NullUserProfileModel);
            return;
        }

        FeatureModelWrapper wrapper = mFeatureModels.get(feature.getName());
        if (wrapper == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_NullWrapper);
            return;
        }

        // call factory method
        updateByOnlineDataAndFeatureModel(feature, onlineData, wrapper.getFeatureModel());
    }
    /*
    public void updateModelByOnlineData(Integer featureID, IOnlineData onlineData) {

        int featureId = featureID.intValue();
        IFeature feature = getFeature(featureId);
        if (feature == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_UnknownFeatureId);
            return;
        }

        if (onlineData == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_NullOnlineData);
            return;
        }

        if (mFeatureModels == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_NullUserProfileModel);
            return;
        }

        FeatureModelWrapper wrapper = mFeatureModels.get(featureID);
        if (wrapper == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_NullWrapper);
            return;
        }

        // call factory method
        updateByOnlineDataAndFeatureModel(featureId, onlineData, wrapper.getFeatureModel());
    }
    //*/


    public synchronized void setFeatureMatrix(String featureName, CoordinateMatrix matrix) {

        if (matrix == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_NullMatrix);
            return;
        }

        mFeatureMatrixs.put(featureName, matrix);

        return;
    }
    /*
    public synchronized void setFeatureMatrix(int featureId, CoordinateMatrix matrix) {

        if (matrix == null) {
            MaxLogger.error(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_NullMatrix);
            return;
        }

        Integer featureID = new Integer(featureId);
        mFeatureMatrixs.put(featureID, matrix);

        return;
    }
    //*/

    public synchronized CoordinateMatrix getFeatureMatrix(String featureName) {

        if (mFeatureMatrixs == null) {
            MaxLogger.info(GenericUserProfileModel.class,
                    ErrMsg.ERR_MSG_NullUserProfileModel);
            return null;
        }

        CoordinateMatrix featureMatrix = mFeatureMatrixs.get(featureName);

        return featureMatrix;
    }
    /*
    public synchronized CoordinateMatrix getFeatureMatrix(int featureId) {

        if (mFeatureMatrixs == null) {
            MaxLogger.info(GenericUserProfileModel.class,
                            ErrMsg.ERR_MSG_NullUserProfileModel);
            return null;
        }

        Integer featureID = new Integer(featureId);
        CoordinateMatrix featureMatrix = mFeatureMatrixs.get(featureID);

        return featureMatrix;
    }
    //*/

    public void updateByOnlineData(IFeature feature, JavaRDD<MatrixEntry> matrixEntriesRDD) {

        if (mFeatureMatrixs == null) {
            mFeatureMatrixs = new HashMap<String, CoordinateMatrix>();
        }

        String featureName = feature.getName();
        CoordinateMatrix coorMatrix = mFeatureMatrixs.get(featureName);

        if ( (coorMatrix != null) && (featureName.equals(UserMerchantFeature.FEATURE_NAME)) ) { // UM matrix
            // debugging starts here
            MaxLogger.debug(GenericUserProfileModel.class, "----------------[updateByOnlineData] Combine Old Matrix " + "----------------");
            // debugging ends here

            JavaRDD<MatrixEntry> existedMatrixEntries = coorMatrix.entries().toJavaRDD();
            matrixEntriesRDD = matrixEntriesRDD.union(existedMatrixEntries);

            JavaPairRDD<Tuple2<Long, Long>, MatrixEntry> pairs = matrixEntriesRDD.mapToPair(
                    new PairFunction<MatrixEntry, Tuple2<Long, Long>, MatrixEntry>() {
                        public Tuple2<Tuple2<Long, Long>, MatrixEntry> call(MatrixEntry entry) {
                            return new Tuple2<Tuple2<Long, Long>, MatrixEntry>(new Tuple2<Long, Long>(entry.i(), entry.j()), entry);
                        }
                    });

            JavaPairRDD<Tuple2<Long, Long>, MatrixEntry> reducedPairs = pairs.reduceByKey(
                    new Function2<MatrixEntry, MatrixEntry, MatrixEntry>() {
                        public MatrixEntry call(MatrixEntry entry_1, MatrixEntry entry_2) throws Exception {
                            long rowId = entry_1.i();
                            long colId = entry_1.j();
                            return new MatrixEntry(rowId, colId, (entry_1.value() + entry_2.value()) );
                        }
                    });

            JavaRDD<MatrixEntry> newEntries = reducedPairs.map(new org.apache.spark.api.java.function.Function<Tuple2<Tuple2<Long, Long>, MatrixEntry>, MatrixEntry>() {
                public MatrixEntry call(Tuple2<Tuple2<Long, Long>, MatrixEntry> tuple2) throws Exception {
                    return tuple2._2();
                }
            });

            if (newEntries == null) {
                MaxLogger.info(GenericUserProfileModel.class,
                        "----------------[updateByOnlineData] RESULT-length = NULL " + "----------------");
            }
            else {

                MaxLogger.info(GenericUserProfileModel.class,
                        "----------------[updateByOnlineData] RESULT-length = [" + newEntries.count() + "]"+ "----------------");

                // debugging starts here
                List<MatrixEntry> entris = newEntries.collect();

                for (MatrixEntry entry : entris) {
                    long userId = entry.i();
                    long merchantId = entry.j();
                    MaxLogger.debug(GenericUserProfileModel.class, featureName +"[i,j] new :" + ", rowId " + userId + ", colId:" + merchantId
                            + ", value:"+ entry.value() );
                }
                // debugging ends here
            }
            coorMatrix = new CoordinateMatrix(newEntries.rdd());
        }
        else {
            // debugging starts here
            MaxLogger.debug(GenericUserProfileModel.class,
                    "----------------[updateByOnlineData] This is a new matrix to replace old one" + featureName + "----------------");
            coorMatrix = new CoordinateMatrix(matrixEntriesRDD.rdd());
            // debugging ends here
        }

        setFeatureMatrix(featureName, coorMatrix);
    }

    /*
    public void updateByOnlineData(int featureId, String featureName, JavaRDD<MatrixEntry> matrixEntriesRDD) {

        if (mFeatureMatrixs == null) {
            mFeatureMatrixs = new HashMap<Integer, CoordinateMatrix>();
        }

        CoordinateMatrix coorMatrix = mFeatureMatrixs.get(featureId);

        if ( (coorMatrix != null) && (featureName.equals(UserMerchantFeature.FEATURE_NAME)) ) { // UM matrix
            MaxLogger.debug(GenericUserProfileModel.class, "----------------[updateByOnlineData] Combine Old Matrix " + "----------------");
            JavaRDD<MatrixEntry> existedMatrixEntries = coorMatrix.entries().toJavaRDD();
            matrixEntriesRDD = matrixEntriesRDD.union(existedMatrixEntries);

            JavaPairRDD<Tuple2<Long, Long>, MatrixEntry> pairs = matrixEntriesRDD.mapToPair(
                    new PairFunction<MatrixEntry, Tuple2<Long, Long>, MatrixEntry>() {
                        public Tuple2<Tuple2<Long, Long>, MatrixEntry> call(MatrixEntry entry) {
                            return new Tuple2<Tuple2<Long, Long>, MatrixEntry>(new Tuple2<Long, Long>(entry.i(), entry.j()), entry);
                        }
                    });

            JavaPairRDD<Tuple2<Long, Long>, MatrixEntry> reducedPairs = pairs.reduceByKey(
                    new Function2<MatrixEntry, MatrixEntry, MatrixEntry>() {
                        public MatrixEntry call(MatrixEntry entry_1, MatrixEntry entry_2) throws Exception {
                            long rowId = entry_1.i();
                            long colId = entry_1.j();
                            return new MatrixEntry(rowId, colId, (entry_1.value() + entry_2.value()) );
                        }
                    });

            JavaRDD<MatrixEntry> newEntries = reducedPairs.map(new org.apache.spark.api.java.function.Function<Tuple2<Tuple2<Long, Long>, MatrixEntry>, MatrixEntry>() {
                public MatrixEntry call(Tuple2<Tuple2<Long, Long>, MatrixEntry> tuple2) throws Exception {
                    return tuple2._2();
                }
            });

            if (newEntries == null) {
                MaxLogger.info(GenericUserProfileModel.class,
                                "----------------[updateByOnlineData] RESULT-length = NULL " + "----------------");
            }
            else {

                MaxLogger.info(GenericUserProfileModel.class,
                                "----------------[updateByOnlineData] RESULT-length = [" + newEntries.count() + "]"+ "----------------");
                // debugging starts here
                List<MatrixEntry> entris = newEntries.collect();

                for (MatrixEntry entry : entris) {
                    long userId = entry.i();
                    long merchantId = entry.j();
                    MaxLogger.debug(GenericUserProfileModel.class, featureId +"[i,j] new :" + ", rowId " + userId + ", colId:" + merchantId
                            + ", value:"+ entry.value() );
                }

                // debugging ends here
            }
            coorMatrix = new CoordinateMatrix(newEntries.rdd());
        }
        else {
            // For debug information
            MaxLogger.debug(GenericUserProfileModel.class,
                            "----------------[updateByOnlineData] This is a new matrix to replace old one" + featureName + "----------------");
            coorMatrix = new CoordinateMatrix(matrixEntriesRDD.rdd());
            //
        }

        setFeatureMatrix(featureId, coorMatrix);
    }
    //*/


    @Override
    public void release() {

        if (mFeatureModels != null) {
            mFeatureModels.clear();
            mFeatureModels = null;
        }

        if (mFeatureMatrixs != null) {
            mFeatureMatrixs.clear();
            mFeatureMatrixs = null;
        }

        if(mFeatures != null) {
            mFeatures.clear();
            mFeatures = null;
        }

        if (mUserProfiles != null) {
            mUserProfiles.clear();
            mUserProfiles = null;
        }
    }
}
