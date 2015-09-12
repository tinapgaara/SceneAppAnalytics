package maxA.util;

import java.util.Formatter;

/**
 * Created by TAN on 7/5/2015.
 */
public class ErrMsg {

    public static final String ERR_MSG_InCorrectInputParamsNum = "Incorrect number of input parameters";

    public static final String ERR_MSG_NotAvroFile = "Not an Avro data file";
    public static final String ERR_MSG_UnknownAppLogMessage = "Unknown App Log Message: [%s]";
    public static final String ERR_MSG_NullLogItemType = "NULL LogItemType ";
    public static final String ERR_MSG_NullHeadersType = "NULL HeadersType ";
    public static final String ERR_MSG_NullActionsType = "NULL ActionsType ";
    public static final String ERR_MSG_NullUserId = "NULL User ID ";
    public static final String ERR_MSG_NullTimestamp= "NULL Timestamp ";
    public static final String ERR_MSG_IllegalContextList = "Illegal Context List: [%s]";
    public static final String ERR_MSG_NullActionsName = "NULL Action's Name ";
    public static final String ERR_MSG_EmptyActionsName = "Empty Action's Name ";
    public static final String ERR_MSG_NullActionsValue = "NULL Action's Value ";
    public static final String ERR_MSG_NullActionsId = "NULL Action's Id ";
    public static final String ERR_MSG_IllegalEndorse = "Illegal Endorse: merchantId or giverId is NULL ";
    public static final String ERR_MSG_IllegalGive = "Illegal Give: attributeName or attributeValue is NULL ";
    public static final String ERR_MSG_IllegalInterestValueInRedis = "Illegal Interest Value in Redis ";
    public static final String ERR_MSG_IllegalInterestRow = "This user has no created interest vector";
    public static final String ERR_MSG_NULLInterestRow = "Get NULL interest vector ";

    public static final String ERR_MSG_UnknownFilter = "Unknown Filter: [%s]";
    public static final String ERR_MSG_DuplicatedRecordFilterName = "Duplicated Record Filter Name: [%s]";

    public static final String ERR_MSG_UnknownHDFSPath = "Unknown HDFS Path: [%s]";

    public static final String ERR_MSG_NullFeatures = "NULL Features";
    public static final String ERR_MSG_NullFeatureVector = "NULL Feature Vector";
    public static final String ERR_MSG_IncompatibleFeatureVector = "Incompatible Feature Vector";
    public static final String ERR_MSG_UnknownFeature = "Unknown Feature: [%s]";
    public static final String ERR_MSG_UnknownFeatureId = "Unknown Feature Id: [%s]";
    public static final String ERR_MSG_UnknownFeatureName = "Unknown Feature Name: [%s]";
    public static final String ERR_MSG_UnregisteredFeature = "Unregistered Feature: [%s]";
    public static final String ERR_MSG_UnregisterFeatureFailed = "Unregister Feature Failed";

    public static final String ERR_MSG_NullTrainData = "NULL Train Data";
    public static final String ERR_MSG_NullTestData = "NULL Test Data";
    public static final String ERR_MSG_NullOnlineData = "NULL Online Data";
    public static final String ERR_MSG_NullClusterData = "NULL Cluster Data";
    public static final String ERR_MSG_NullRecommenderData = "NULL Recommender Data";
    public static final String ERR_MSG_NULLUserEndorseData = "NULL User's endorse data";

    public static final String ERR_MSG_IllegalUserAttributeName = "Illegal User Attribute Name: [%s]";

    public static final String ERR_MSG_NullUserProfileModel = "NULL User Profile Model";
    public static final String ERR_MSG_NullUserProfileModelUpdater = "NULL User Profile Model Updater";
    public static final String ERR_MSG_UnknownUserProfileModelUpdater = "Unknown User Profile Model Updater";
    public static final String ERR_MSG_NullMatrix = "NULL Matrix";
    public static final String ERR_MSG_NullWrapper = "NULL Model Wrapper";

    public static final String ERR_MSG_NullClusterModel= "NULL Cluster Model";
    public static final String ERR_MSG_NullClusterModelUpdater = "NULL Cluster Model Updater";
    public static final String ERR_MSG_UnknownClusterNum = "Unknown Cluster Number: [%s]";
    public static final String ERR_MSG_NullClusterAssignment = "NULL Cluster Assignment";

    public static final String ERR_MSG_NullRecommender = "NULL recommender";

    public static final String ERR_MSG_UnKownUserProfileModel = "Unknown User Profile Model. You can use :" +
                                                                "1 - LinearRegressionModel; 2 - LogisticRegressionModel;" +
                                                                "3 - NaiveBayesModel; 4 - SVMModel";


    public static String msg(String msg, String param) {

        Formatter formatter = new Formatter();
        return formatter.format(msg, param).toString();
    }
}
