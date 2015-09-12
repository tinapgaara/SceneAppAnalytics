package maxA.main;

import maxA.userProfile.*;

/**
 * Created by TAN on 7/17/2015.
 */

public class UserFeatureVectorUpdater {

    public UserFeatureVectorUpdater() {
        // nothing to do here
    }

    public void updateByAppLog(String hdfsPath)
            throws UserProfileException {
/*
        LR_UserProfileModel userProfileModel = LR_UserProfileModel.getInstance();

        SparkClient sparkClient = SparkClient.getInstance();
        AppLogFilter filter = new AppLogFilter();
        IDecoder decoder = new AppLogDecoder(filter);

        IFeatureFilter featureFilter;

        IFeature[] features = userProfileModel.getAllFeatures();// get all features which have been registered when building the model
        for (IFeature feature : features) {
            featureFilter = feature.getFeatureFilter();
            featureFilter.setTrainDataFilterFlag(false);// false: suggest that it is used for testing ; true: training

            decoder.registerFeatureFilter(featureFilter);
        }

        sparkClient.processStreamFiles(hdfsPath, decoder);
*/
        // Generate feature vectors (i.e., test data) for each feature
        // Todo: how to separate trainData and testData, can be done after implementing how to generate traindata.
/*
        int featureId;
        IOnlineDataGenerator testDataGenerator = LR_OnlineDataGenerator.getInstance();

        for (IFeature feature : features) {
            featureFilter = feature.getFeatureFilter();
            featureId = userProfileModel.getFeatureIdByName(feature.getName());
            JavaRDD<Tuple4<Long, Long, Integer, Double>> rdd = testDataGenerator.generateTestDataByOneAppLog(feature, featureFilter.getInputAppLog());
            userProfileModel.predict(featureId,rdd); //Todo
        }
*/
        /*
        for (IFeature feature : features) {

            featureFilter = feature.getFeatureFilter();
            featureVector = testDataGenerator.generateTestData(feature, featureFilter.getInputData());//
            for (Integer userID : featureVector.keySet()) {
                userProfileModel.setFeatureVectors(userID, feature,
                        featureVector.get(userID));
            }
        }
        */
    }

}