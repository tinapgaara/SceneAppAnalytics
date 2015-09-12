package maxA.main;

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
import maxA.userProfile.impl.GenericUserProfile;
import maxA.userProfile.impl.linearRegression.LR_TrainDataGenerator;
import maxA.userProfile.impl.linearRegression.LR_UserProfileModel;
import maxA.userProfile.impl.logisticRegression.LogR_TrainDataGenerator;
import maxA.userProfile.impl.logisticRegression.LogR_UserProfileModel;
import maxA.userProfile.impl.naiveBayes.NB_TrainDataGenerator;
import maxA.userProfile.impl.naiveBayes.NB_UserProfileModel;
import maxA.userProfile.impl.svm.SVM_TrainDataGenerator;
import maxA.userProfile.impl.svm.SVM_UserProfileModel;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * Created by TAN on 7/5/2015.
 */
public class UserProfileModelBuilder {

    private static String TEST_APP_ASK_FILE_PATH = "/test/15-08-03/AppLogs-events.1438637664397";

    public UserProfileModelBuilder() {
        // nothing to do here
    }

    public static void main(String[] args) {
        UserProfileModelBuilder builder = new UserProfileModelBuilder();
        try {
            builder.buildFromAppLogs(Constants.HDFS_PATH + TEST_APP_ASK_FILE_PATH);
        } catch (UserProfileException e){
            e.printStackTrace();
        }
    }

    public IUserProfileModel buildFromAppLogs(String filePath)
            throws UserProfileException {

        // ************ LR_UserProfileModel ***************
        // 1. Intialize LR_userProfileModel
        /*
        LR_UserProfileModel userProfileModel = LR_UserProfileModel.getInstance();
        // 2. Prepare feature(s) for the user profile model
        IFeature umFeature = new UserMerchantFeature();
        IFeatureFilter umFilter = new UserMerchantFeatureFilter();
        umFilter.setTrainDataFilterFlag(true);
        umFeature.setFeatureFilter(umFilter);
        int umFeatureId = userProfileModel.registerFeature(umFeature);

        IFeature uuFeature = new UserUserFeature();
        IFeatureFilter uuFilter = new UserUserFeatureFilter();
        uuFilter.setTrainDataFilterFlag(true);
        uuFeature.setFeatureFilter(uuFilter);
        int uuFeatureId = userProfileModel.registerFeature(uuFeature);

        //3.  Read input data
        SparkClient sparkClient = SparkClient.getInstance();
        AppLogFilter filter = new AppLogFilter();
        IDecoder decoder = new AppLogDecoder(filter);

        // 4. register record filters
        decoder.registerFeatureFilter(umFilter);// filter all data related to X and y in UserMerchant feature
        decoder.registerFeatureFilter(uuFilter);
        sparkClient.readBinaryFileFromHDFS(filePath, decoder);

        //5.Generate train data and train feature model for each feature
        LR_TrainDataGenerator trainDataGenerator = LR_TrainDataGenerator.getInstance();
        ITrainData trainData = trainDataGenerator.generateTrainDataByAppLog(umFeature, umFilter.getInputAppLogs());
        userProfileModel.trainFeatureModel(umFeatureId, trainData);
*/
/*
        // ************ SVM_UserProfileModel ***************
        // 1. Intialize LR_userProfileModel
        SVM_UserProfileModel userProfileModel = SVM_UserProfileModel.getInstance();
        // 2. Prepare feature(s) for the user profile model
        IFeature umFeature = new UserMerchantFeature();
        IFeatureFilter umFilter = new UserMerchantFeatureFilter();
        umFilter.setTrainDataFilterFlag(true);
        umFeature.setFeatureFilter(umFilter);
        int umFeatureId = userProfileModel.registerFeature(umFeature);
        //3.  Read input data
        SparkClient sparkClient = SparkClient.getInstance();
        AppLogFilter filter = new AppLogFilter();
        IDecoder decoder = new AppLogDecoder(filter);
        // 4. register record filters
        decoder.registerFeatureFilter(umFilter);// filter all data related to X and y in UserMerchant feature
        sparkClient.readBinaryFileFromHDFS(filePath, decoder); // previous: IRecord
        //5.Generate train data and train feature model for each feature
        ITrainDataGenerator trainDataGenerator = SVM_TrainDataGenerator.getInstance();
        ITrainData trainData = trainDataGenerator.generateTrainDataByAppLog(umFeature, umFilter.getInputAppLogs());
        userProfileModel.trainFeatureModel(umFeatureId, trainData);
*/

/*
        // ************ LogisticRegression_UserProfileModel ***************
        // 1. Intialize LogR_userProfileModel
        LogR_UserProfileModel userProfileModel = LogR_UserProfileModel.getInstance();
        // 2. Prepare feature(s) for the user profile model
        IFeature umFeature = new UserMerchantFeature();
        IFeatureFilter umFilter = new UserMerchantFeatureFilter();
        umFilter.setTrainDataFilterFlag(true);
        umFeature.setFeatureFilter(umFilter);
        int umFeatureId = userProfileModel.registerFeature(umFeature);
        //3.  Read input data
        SparkClient sparkClient = SparkClient.getInstance();
        AppLogFilter filter = new AppLogFilter();
        IDecoder decoder = new AppLogDecoder(filter);
        // 4. register record filters
        decoder.registerFeatureFilter(umFilter);// filter all data related to X and y in UserMerchant feature
        sparkClient.readBinaryFileFromHDFS(filePath, decoder); // previous: IRecord
        //5.Generate train data and train feature model for each feature
        ITrainDataGenerator trainDataGenerator = LogR_TrainDataGenerator.getInstance();
        ITrainData trainData = trainDataGenerator.generateTrainDataByAppLog(umFeature, umFilter.getInputAppLogs());
        userProfileModel.trainFeatureModel(umFeatureId, trainData);
*/

/*
        // ************ NaiveBayes_UserProfileModel ***************
        // 1. Intialize NB_userProfileModel
        NB_UserProfileModel userProfileModel = NB_UserProfileModel.getInstance();
        // 2. Prepare feature(s) for the user profile model
        IFeature umFeature = new UserMerchantFeature();
        IFeatureFilter umFilter = new UserMerchantFeatureFilter();
        umFilter.setTrainDataFilterFlag(true);
        umFeature.setFeatureFilter(umFilter);
        int umFeatureId = userProfileModel.registerFeature(umFeature);
        //3.  Read input data
        SparkClient sparkClient = SparkClient.getInstance();
        AppLogFilter filter = new AppLogFilter();
        IDecoder decoder = new AppLogDecoder(filter);
        // 4. register record filters
        decoder.registerFeatureFilter(umFilter);// filter all data related to X and y in UserMerchant feature
        sparkClient.readBinaryFileFromHDFS(filePath, decoder); // previous: IRecord
        //5.Generate train data and train feature model for each feature
        ITrainDataGenerator trainDataGenerator = NB_TrainDataGenerator.getInstance();
        ITrainData trainData = trainDataGenerator.generateTrainDataByAppLog(umFeature, umFilter.getInputAppLogs());
        userProfileModel.trainFeatureModel(umFeatureId, trainData);
*/

        // Now we finish building the user profile model !
//        return userProfileModel;
        return null;
    }

}
