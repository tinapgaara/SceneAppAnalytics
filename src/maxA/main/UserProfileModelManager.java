package maxA.main;

import maxA.cluster.IClusteringModel;
import maxA.cluster.IUserClusterModel;
import maxA.cluster.IUserClusterUpdater;
import maxA.cluster.impl.PIC.PIC_ClusteringModel;
import maxA.cluster.impl.PIC.PIC_UserClusterModel;
import maxA.cluster.impl.PIC.PIC_UserClusterUpdater;
import maxA.common.Constants;
import maxA.io.AppLogRecord;
import maxA.io.decoder.IDecoder;
import maxA.io.decoder.impl.AppLogDecoder;
import maxA.io.filter.impl.AppLogFilter;
import maxA.io.sparkClient.SparkClient;
import maxA.userProfile.*;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import maxA.userProfile.feature.userMerchant.UserMerchantFeatureFilter;
import maxA.userProfile.feature.userUser.UserUserFeature;
import maxA.userProfile.feature.userUser.UserUserFeatureFilter;
import maxA.userProfile.impl.linearRegression.*;
import maxA.userProfile.impl.logisticRegression.LogR_OnlineDataGenerator;
import maxA.userProfile.impl.logisticRegression.LogR_TrainDataGenerator;
import maxA.userProfile.impl.logisticRegression.LogR_UserProfileModel;
import maxA.userProfile.impl.logisticRegression.LogR_UserProfileModelUpdater;
import maxA.userProfile.impl.naiveBayes.NB_OnlineDataGenerator;
import maxA.userProfile.impl.naiveBayes.NB_TrainDataGenerator;
import maxA.userProfile.impl.naiveBayes.NB_UserProfileModel;
import maxA.userProfile.impl.naiveBayes.NB_UserProfileModelUpdater;
import maxA.userProfile.impl.svm.SVM_OnlineDataGenerator;
import maxA.userProfile.impl.svm.SVM_TrainDataGenerator;
import maxA.userProfile.impl.svm.SVM_UserProfileModel;
import maxA.userProfile.impl.svm.SVM_UserProfileModelUpdater;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import org.apache.spark.api.java.JavaRDD;

import java.util.Set;

/**
 * Created by max2 on 7/5/2015.
 */
public class UserProfileModelManager {

    private static UserProfileModelManager mInstance = null;

    private IUserProfileModel mUserProfileModel;
    private IUserClusterModel mUserClusterModel;

    public static UserProfileModelManager getInstance(int modelType) {

        if (mInstance == null) {
            mInstance = new UserProfileModelManager(modelType);
        }
        return mInstance;
    }

    private UserProfileModelManager(int modelType) {

        if (modelType == Constants.LinearRegressionModelNo) {
            mUserProfileModel = LR_UserProfileModel.getInstance();
        }
        else if (modelType == Constants.LogisticRegressionModelNo) {
            mUserProfileModel = LogR_UserProfileModel.getInstance();
        }
        else if (modelType == Constants.NaiveBayesModelNo) {
            mUserProfileModel = NB_UserProfileModel.getInstance();
        }
        else if (modelType == Constants.SVMModelNo) {
            mUserProfileModel = SVM_UserProfileModel.getInstance();
        }
        else {
            MaxLogger.info(UserProfileModelManager.class,
                            ErrMsg.ERR_MSG_UnKownUserProfileModel);
        }

        IClusteringModel clusterModel = PIC_ClusteringModel.getInstance();
        mUserClusterModel = new PIC_UserClusterModel(clusterModel);
    }

    public IUserProfileModel getUserProfileModel() {

        return mUserProfileModel;
    }

    public IUserClusterModel getUserClusterModel() {
        return mUserClusterModel;
    }

    public IUserProfileModel buildFromAppLogs(String filePath, int modelType)
            throws UserProfileException {

        // 1.  Prepare feature(s) for the user profile model
        IFeature umFeature = new UserMerchantFeature();
        IFeatureFilter umFilter = UserMerchantFeatureFilter.getInstance();
        // the training data filter flag is set to true because we are in the build method and not update method
        umFilter.setTrainDataFilterFlag(true);
        umFeature.setFeatureFilter(umFilter);
        /*
        int umFeatureId = mUserProfileModel.registerFeature(umFeature);
        //*/
        mUserProfileModel.registerFeature(umFeature);

        IFeature uuFeature = new UserUserFeature();
        IFeatureFilter uuFilter = UserUserFeatureFilter.getInstance();
        uuFilter.setTrainDataFilterFlag(true);
        uuFeature.setFeatureFilter(uuFilter);
        /*
        int uuFeatureId = mUserProfileModel.registerFeature(uuFeature);
        //*/
        mUserProfileModel.registerFeature(uuFeature);

        // 2.  Read input data
        SparkClient sparkClient = SparkClient.getInstance();
        AppLogFilter filter = new AppLogFilter();
        IDecoder decoder = new AppLogDecoder(filter);

        // 3. register record filters and decode the applogs
        decoder.registerFeatureFilter(umFilter);// filter all data related to X and y in UserMerchant feature
        decoder.registerFeatureFilter(uuFilter); // filter all data related to UserUser Feature
        sparkClient.readFileFromHDFS(filePath, decoder); // map the applogs to applogrecords

        ITrainDataGenerator trainDataGenerator = null;

        if (modelType == Constants.LinearRegressionModelNo) {
            // ************ LR_UserProfileModel ***************
            // 4.Generate train data and train feature model for each feature
            trainDataGenerator = LR_TrainDataGenerator.getInstance();
        }
        else if (modelType == Constants.LogisticRegressionModelNo) {
            // ************ LogisticRegression_UserProfileModel ***************
            // 4.Generate train data and train feature model for each feature
            trainDataGenerator = LogR_TrainDataGenerator.getInstance();
        }
        else if (modelType == Constants.NaiveBayesModelNo) {
            // ************ NaiveBayes_UserProfileModel ***************
            // 4.Generate train data and train feature model for each feature
            trainDataGenerator = NB_TrainDataGenerator.getInstance();
        }
        else if (modelType == Constants.SVMModelNo) {
            // ************ SVM_UserProfileModel ***************
            // 4.Generate train data and train feature model for each feature
            trainDataGenerator = SVM_TrainDataGenerator.getInstance();
        }
        else {
            MaxLogger.info(UserProfileModelManager.class,
                            ErrMsg.ERR_MSG_UnKownUserProfileModel);
        }

        // 4.1 Feature Vum
        JavaRDD<AppLogRecord> inputData = umFilter.getInputAppLogs();
        if (inputData != null) {
            // ITrainData trainData = trainDataGenerator.generateTrainDataByAppLogs(umFeature, inputData);// trainData: [1,0,...] 0  <- inpitData: give
            /*
            mUserProfileModel.trainFeatureModel(umFeatureId, trainData);
            //*/
            ITrainData trainData =  trainDataGenerator.generateRandomTrainingData(umFeature);
            mUserProfileModel.trainFeatureModel(umFeature, trainData);
        } else {
            MaxLogger.error(UserProfileModelManager.class, ErrMsg.ERR_MSG_NullTrainData);
        }
        // 4.2 Feature: Vuu
        inputData = uuFilter.getInputAppLogs();
        if (inputData != null) {
            // the training data will be null over here
            ITrainData trainData = trainDataGenerator.generateTrainDataByAppLogs(uuFeature, inputData);
            /*
            mUserProfileModel.trainFeatureModel(uuFeatureId, trainData);
            //*/
            mUserProfileModel.trainFeatureModel(uuFeature, trainData);
        } else {
            MaxLogger.error(UserProfileModelManager.class, ErrMsg.ERR_MSG_NullTrainData);
        }

        mUserProfileModel.getAllFeatures();
        // Now we finish building the user profile model !
        return mUserProfileModel;
    }

    public void updateByAppLogs(String hdfsPath, int modelType, int testFlag)
            throws UserProfileException {

        SparkClient sparkClient = SparkClient.getInstance();
        AppLogFilter filter = new AppLogFilter();
        IDecoder decoder = new AppLogDecoder(filter);

        IFeatureFilter featureFilter;

        /*
        IFeature[] features = mUserProfileModel.getAllFeatures(); // get all features which have been registered when building the model
        //*/
        Set<IFeature> features = mUserProfileModel.getAllFeatures();
        for (IFeature feature : features) {
            featureFilter = feature.getFeatureFilter();
            featureFilter.setTrainDataFilterFlag(false);

            decoder.registerFeatureFilter(featureFilter);
        }

        StreamProcessor streamProcessor = StreamProcessor.getInstance();

        IUserProfileModelUpdater userProfileModelUpdater = null;

        if (modelType == Constants.LinearRegressionModelNo) {
            userProfileModelUpdater = LR_UserProfileModelUpdater.getInstance(mUserProfileModel);
            userProfileModelUpdater.setOnlineDataGenerator(LR_OnlineDataGenerator.getInstance());
        }
        else if (modelType == Constants.LogisticRegressionModelNo) {
            userProfileModelUpdater = LogR_UserProfileModelUpdater.getInstance(mUserProfileModel);
            userProfileModelUpdater.setOnlineDataGenerator(LogR_OnlineDataGenerator.getInstance());
        }
        else if (modelType == Constants.NaiveBayesModelNo) {
            userProfileModelUpdater = NB_UserProfileModelUpdater.getInstance(mUserProfileModel);
            userProfileModelUpdater.setOnlineDataGenerator(NB_OnlineDataGenerator.getInstance());
        }
        else if (modelType == Constants.SVMModelNo) {
            userProfileModelUpdater =  SVM_UserProfileModelUpdater.getInstance(mUserProfileModel);
            userProfileModelUpdater.setOnlineDataGenerator(SVM_OnlineDataGenerator.getInstance());
        }
        else {
            MaxLogger.info(UserProfileModelManager.class,
                            ErrMsg.ERR_MSG_UnKownUserProfileModel);
            return;
        }

        IUserClusterUpdater userClusterUpdater = PIC_UserClusterUpdater.getInstance(mUserClusterModel);
        userClusterUpdater.setUserProfileModel(mUserProfileModel);

        streamProcessor.setUserProfileModelUpdater(userProfileModelUpdater);
        streamProcessor.setUserClusterUpdater(userClusterUpdater);

        // we should not change the order of starting the threads
        StreamProcessingThread streamProcessingThread = new StreamProcessingThread(decoder, streamProcessor, testFlag);
        streamProcessingThread.start();

        SparkStreamThread sparkStreamThread = new SparkStreamThread(sparkClient, hdfsPath,
                                                                    streamProcessingThread, testFlag);
        sparkStreamThread.start();
        sparkStreamThread.setPriority(Thread.MAX_PRIORITY);

    }
}
