package main.java;

import maxA.common.Constants;
import maxA.common.Constants_TestOnly;
import maxA.io.AppLogRecord;
import maxA.main.UserProfileModelManager;
import maxA.recommender.ALS.impl.ALS_Recommender;
import maxA.recommender.IRecommender;
import maxA.userProfile.IUserProfileModel;
import maxA.userProfile.UserProfileException;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by TAN on 7/6/2015.
 */
public class TestUserProfileModelBuilder {

    private final static int SLEEP_TIME_InMilliseconds = 100000;

    public static void main(String[] args) {

        if (args.length != Constants.TESTModelNum) {
            MaxLogger.error(TestUserProfileModelBuilder.class,
                            ErrMsg.ERR_MSG_InCorrectInputParamsNum);
            MaxLogger.info(TestUserProfileModelBuilder.class,
                            "\nPlease enter " + Constants.TESTModelNum + " parameters: \n" +
                                    "the 1st param is the model No: (1 - LinearRegressionModel; 2 - LogisticRegressionModel; " +
                                                                    "3 - NaiveBayesModel; 4 - SVMModel) \n" +
                                    "the 2nd param is the test flag: (0 - Regular model; 1 - Test model) \n" +
                                    "the 3rd param is the userID, \n" +
                                    "the 4th param is the maxNum of recommendations.");

            return;
        }

        int modelType = Integer.parseInt(args[0]);
        int testFlag = Integer.parseInt(args[1]);
        long userId = Long.parseLong(args[2]);
        int maxNumOfRecommendations = Integer.parseInt(args[3]);

        MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "------  [modelType]:" + modelType + ", [testFlag]:" + testFlag + ", [userId]: " + userId +
                                "[recommendationNum]:" + maxNumOfRecommendations);

        TestUserProfileModelBuilder builder = new TestUserProfileModelBuilder();

        builder.test(userId, modelType, testFlag, maxNumOfRecommendations);
    }

    public void test(long userId, int modelType, int testFlag, int maxNumOfMerchants) {

        UserProfileModelManager manager = UserProfileModelManager.getInstance(modelType);

        IRecommender recommender = ALS_Recommender.getInstance(
                                    manager.getUserClusterModel(),
                                    manager.getUserProfileModel());

        String trainFilePath = Constants.HDFS_PATH_DEV + Constants.TEST_TRAIN_FILE_PATH;
        String testStreamPath = Constants.HDFS_PATH_DEV;

        if (testFlag == 1) {
            Constants_TestOnly.TEST_FLAG = true;
        }

        IUserProfileModel model = null;
        try {
            model = manager.buildFromAppLogs(trainFilePath, modelType);
            manager.updateByAppLogs(testStreamPath, modelType, testFlag);

            int cRecommendTimes = 20;
            for (int i = 0; i < cRecommendTimes; i++) {
                recommender.recMerchants2User(userId, maxNumOfMerchants);
                try {
                    // debugging starts here
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "----------------[After recommending for " + i + "th user, sleep for a while]");
                    // debugging ends here
                    Thread.sleep(SLEEP_TIME_InMilliseconds);
                }
                catch(InterruptedException ex) {
                    // nothing to do here
                }
            }
        }
        catch (UserProfileException ex) {
            MaxLogger.error(TestUserProfileModelBuilder.class, ex.getMessage());
        }
    }

    public static AppLogRecord[] generateAppLogRecords_TestOnly(int maxNumRecords) {

        String actionName, actionValue;
        String contextName, contextValue, contextName_2, contextValue_2;
        double timestamp = 0;

        long _UserId = 0, _ActionId = 0;
        double _Lat = 0, _Lng = 0;

        int numGenerateAppLogRecords = Math.min(maxNumRecords,
                                                Constants_TestOnly.MAX_NUM_GenerateAppLogRecords);
        AppLogRecord[] appLogRecords = new AppLogRecord[numGenerateAppLogRecords];

        // test smaller data set
        for (int i = 0; i < numGenerateAppLogRecords; i++) {

            actionName = null;
            actionValue = null;
            contextName = null;
            contextValue = null;
            contextName_2 = null;
            contextValue_2 = null;

            switch (i) {

                case 0: // add anotherUserId:2
                    actionName = "add";
                    actionValue = "people";
                    contextName = "planID";
                    contextValue = "11";
                    _UserId = 14;
                    _ActionId = 2;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: add person, userId:"
                                    + _UserId + " , anotherUserId:" + _ActionId);
                    break;

                case 1:
                    actionName = "give";
                    actionValue = null;
                    contextName = "Queue";
                    contextValue = "short";
                    _UserId = 12;
                    _ActionId = 13;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: queue short, userId:"
                                    + _UserId + ",  merchantId:" + _ActionId);
                    break;

                case 2: // reply rating good
                    actionName = "reply";
                    actionValue = null;
                    contextName = "Queue";
                    contextValue = "short";
                    _UserId = 12;
                    _ActionId = 13;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: reply good, userId:"
                                    + _UserId + ", merchantId:" + _ActionId);
                    break;


                case 3: // enter merchant
                    actionName = "enter";
                    actionValue = "merchant";
                    contextName = null;
                    contextValue = null;
                    _UserId = 12;
                    _ActionId = 13;
                    timestamp = 100;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                            "---------------- [generateAppLogRecords_TestOnly]: enter merchant, userId:"
                                    + _UserId + " , merchantId:" + _ActionId);
                    break;

                case 4: // leave merchant
                    actionName = "leave";
                    actionValue = "merchant";
                    contextName = null;
                    contextValue = null;
                    _UserId = 12;
                    _ActionId = 13;
                    timestamp = 233;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                            "---------------- [generateAppLogRecords_TestOnly]: leave merchant, userId:" + _UserId
                                    + ", merchantId:" + _ActionId);
                    break;

                case 5: // ask queue
                    actionName = "ask";
                    actionValue = "Queue";
                    contextName = "merchantID";
                    contextValue = "13";
                    _UserId = 12;
                    _ActionId = 13;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: ask queue, userId:" + _UserId
                                    + ", merchantId:" + contextValue);
                    break;

                case 6: // endorse true userId:12, anotherUserId:1, merchantId: 66
                    actionName = "endorse";
                    actionValue = "true";
                    contextName = "merchantID";
                    contextValue = "66";
                    contextName_2 = "giverID";
                    contextValue_2 = "1";
                    _UserId = 12;
                    _ActionId = 847;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: endorse, userId:" + _UserId
                                    + ", merchantId:" + contextValue + ", anotherUserId:" + contextValue_2);
                    break;

                case 7: // interest add
                    actionName = "interest";
                    actionValue = "add";
                    contextName = null;
                    contextValue = null;
                    _UserId = 12;
                    _ActionId = 3;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: add interest, userId:" + _UserId
                                    + ", merchantId: 26027");
                    break;

                // userId: 14, merchantId: 15
                case 8:
                    actionName = "give";
                    actionValue = null;
                    contextName = "Rating";
                    contextValue = "good";
                    _UserId = 14;
                    _ActionId = 15;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: rating good, userId:" + _UserId
                                    + ", merchantId:" + _ActionId);
                    break;

                case 9:
                    actionName = "give";
                    actionValue = null;
                    contextName = "Queue";
                    contextValue = "short";
                    _UserId = 14;
                    _ActionId = 15;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: queue short, userId:" + _UserId
                                    + ", merchantId:" + _ActionId);
                    break;

                case 10: // reply rating good
                    actionName = "reply";
                    actionValue = null;
                    contextName = "Queue";
                    contextValue = "short";
                    _UserId = 14;
                    _ActionId = 15;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: reply good, userId:" + _UserId
                                    + ", merchantId:" + _ActionId);
                    break;

                case 11: // ask queue
                    actionName = "ask";
                    actionValue = "Queue";
                    contextName = "merchantID";
                    contextValue = "15";
                    _UserId = 14;
                    _ActionId = 15;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: ask queue, userId:" + _UserId
                                    + ", merchantId:" + contextValue);
                    break;

                case 12: // endorse true  userId:14, anotherUserId:1
                    actionName = "endorse";
                    actionValue = "true";
                    contextName = "merchantID";
                    contextValue = "66";
                    contextName_2 = "giverID";
                    contextValue_2 = "1";
                    _UserId = 14;
                    _ActionId = 851;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: endorse, userId:" + _UserId
                                    + ", merchantId:" + contextValue + ", anotherUserId:" + contextValue_2);
                    break;

                case 13: // add merchant
                    actionName = "add";
                    actionValue = "merchant";
                    contextName = "planID";
                    contextValue = "11";
                    _UserId = 14;
                    _ActionId = 66;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: add merchant, userId:" + _UserId
                                    + ", merchantId:" + _ActionId);
                    break;

                case 14:
                    actionName = "give";
                    actionValue = null;
                    contextName = "Rating";
                    contextValue = "good";
                    _UserId = 12;
                    _ActionId = 13;
                    MaxLogger.info(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: rating good, userId:" +_UserId
                                    + ", merchantId:" + _ActionId);
                    break;

                case 15:
                    actionName = "add";
                    actionValue = "people";
                    contextName = "planID";
                    contextValue = "11";
                    _UserId = 12;
                    _ActionId = 14;
                    MaxLogger.debug(TestUserProfileModelBuilder.class,
                                    "---------------- [generateAppLogRecords_TestOnly]: add person, userId:" + _UserId
                                    + ", anotherUserId:" + _ActionId);
                    break;

            } // end of switch

            _Lat = 12.12;
            _Lng = 12.12;

            appLogRecords[i] = new AppLogRecord(_UserId, _ActionId,
                    actionName, actionValue, _Lat, _Lng, timestamp,
                    contextName, contextValue,
                    contextName_2, contextValue_2);
        } // end of for

        return appLogRecords;
    }

    public static AppLogRecord generateAppLogRecord_TestOnly(int testDataCase) {

        String actionName = null, actionValue = null;
        String contextName = null, contextValue = null, contextName_2 = null, contextValue_2 = null;

        long _UserId = 0, _ActionId = 0;
        double _Lat = 0, _Lng = 0;
        double timestamp = 0;

        switch (testDataCase) {

            case 0: // add anotherUserId:43
                actionName = "add";
                actionValue = "people";
                contextName = "planID";
                contextValue = "11";
                _UserId = 14;
                _ActionId = 2;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: add person, userId:"
                                + _UserId + " , anotherUserId:" + _ActionId);
                break;

            case 1:
                actionName = "give";
                actionValue = null;
                contextName = "Queue";
                contextValue = "short";
                _UserId = 12;
                _ActionId = 13;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: queue short, userId:"
                                + _UserId + ",  merchantId:" + _ActionId);
                break;

            case 2: // reply rating good
                actionName = "reply";
                actionValue = null;
                contextName = "Queue";
                contextValue = "short";
                _UserId = 12;
                _ActionId = 13;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: reply good, userId:"
                                + _UserId + ", merchantId:" + _ActionId);
                break;

            case 3: // ask queue
                actionName = "ask";
                actionValue = "Queue";
                contextName = "merchantID";
                contextValue = "13";
                _UserId = 12;
                _ActionId = 13;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: ask queue, userId:" + _UserId
                                + ", merchantId:" + contextValue);
                break;

            case 4: // endorse true userId:12, anotherUserId:1, merchantId: 66
                actionName = "endorse";
                actionValue = "true";
                contextName = "merchantID";
                contextValue = "66";
                contextName_2 = "giverID";
                contextValue_2 = "1";
                _UserId = 12;
                _ActionId = 847;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: endorse, userId:" + _UserId
                                + ", merchantId:" + contextValue + ", anotherUserId:" + contextValue_2);
                break;

            case 5: // interest add
                actionName = "interest";
                actionValue = "add";
                contextName = null;
                contextValue = null;
                _UserId = 12;
                _ActionId = 13;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: add interest, userId:" + _UserId
                                + ", merchantId: 66, 2");
                break;

            // userId: 14, merchantId: 15
            case 6:
                actionName = "give";
                actionValue = null;
                contextName = "Rating";
                contextValue = "good";
                _UserId = 14;
                _ActionId = 15;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: rating good, userId:" + _UserId
                                + ", merchantId:" + _ActionId);
                break;

            case 7:
                actionName = "give";
                actionValue = null;
                contextName = "Queue";
                contextValue = "short";
                _UserId = 14;
                _ActionId = 15;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: queue short, userId:" + _UserId
                                + ", merchantId:" + _ActionId);
                break;

            case 8: // reply rating good
                actionName = "reply";
                actionValue = null;
                contextName = "Queue";
                contextValue = "short";
                _UserId = 14;
                _ActionId = 15;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: reply good, userId:" + _UserId
                                + ", merchantId:" + _ActionId);
                break;

            case 9: // ask queue
                actionName = "ask";
                actionValue = "Queue";
                contextName = "merchantID";
                contextValue = "15";
                _UserId = 14;
                _ActionId = 15;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: ask queue, userId:" + _UserId
                                + ", merchantId:" + contextValue);
                break;

            case 10: // endorse true  userId:14, anotherUserId:1
                actionName = "endorse";
                actionValue = "true";
                contextName = "merchantID";
                contextValue = "66";
                contextName_2 = "giverID";
                contextValue_2 = "1";
                _UserId = 14;
                _ActionId = 851;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: endorse, userId:" + _UserId
                                + ", merchantId:" + contextValue + ", anotherUserId:" + contextValue_2);
                break;

            case 11: // add merchant
                actionName = "add";
                actionValue = "merchant";
                contextName = "planID";
                contextValue = "11";
                _UserId = 14;
                _ActionId = 66;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: add merchant, userId:" + _UserId
                                + ", merchantId:" + _ActionId);
                break;

            case 12:
                actionName = "give";
                actionValue = null;
                contextName = "Rating";
                contextValue = "good";
                _UserId = 12;
                _ActionId = 13;
                MaxLogger.info(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: rating good, userId:" +_UserId
                                + ", merchantId:" + _ActionId);
                break;

            case 13:
                actionName = "add";
                actionValue = "people";
                contextName = "planID";
                contextValue = "11";
                _UserId = 12;
                _ActionId = 14;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: add person, userId:" + _UserId
                                + ", anotherUserId:" + _ActionId);
                break;

            case 14: // userId:12, merchantId: 2
                actionName = "give";
                actionValue = null;
                contextName = "Rating";
                contextValue = "good";
                _UserId = 12;
                _ActionId = 2;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly]: rating good, userId:" + _UserId
                                + ", merchantId:" + _ActionId);
                break;

            default:
                actionName = "give";
                actionValue = null;
                contextName = "Rating";
                contextValue = "good";
                _UserId = 12;
                _ActionId = 2;
                MaxLogger.debug(TestUserProfileModelBuilder.class,
                        "---------------- [generateAppLogRecord_TestOnly default]: rating good, userId:" + _UserId
                                + ", merchantId:" + _ActionId);
                break;

        } // end of switch

        _Lat = 12.12;
        _Lng = 12.12;

        AppLogRecord appLogRecord = new AppLogRecord(_UserId, _ActionId,
                actionName, actionValue, _Lat, _Lng, timestamp,
                contextName, contextValue,
                contextName_2, contextValue_2);

        return appLogRecord;
    }
}
