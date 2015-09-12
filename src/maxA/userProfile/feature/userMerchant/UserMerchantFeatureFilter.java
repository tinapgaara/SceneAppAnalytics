package maxA.userProfile.feature.userMerchant;

import maxA.io.AppLogRecord;
import maxA.io.sparkClient.SparkContext;
import maxA.userProfile.feature.GenericFeatureFilter;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by TAN on 7/8/2015.
 */
public class UserMerchantFeatureFilter extends GenericFeatureFilter {

    public static final String NAME = "UserMerchantFeatureFilter";

    private static UserMerchantFeatureFilter m_instance = null;

    private static JavaRDD<AppLogRecord> mAppLogRecords_Train = null;
    private static JavaRDD<AppLogRecord> mAppLogRecords_Test = null;

    public static UserMerchantFeatureFilter getInstance() {

        if (m_instance == null) {
            m_instance = new UserMerchantFeatureFilter();
        }
        return m_instance;
    }

    private UserMerchantFeatureFilter() {
        // nothing to do here
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isUsefulAppLog(AppLogRecord record) {

        int actionName = record.getActionName();
        boolean isUseful = false;
        if (actionName == -1) {
            return isUseful;
        }

        if ((actionName == AppLogRecord.ACTION_give) || (actionName == AppLogRecord.ACTION_reply) || (actionName == AppLogRecord.ACTION_ask)
                || (actionName == AppLogRecord.ACTION_vote) || (actionName == AppLogRecord.ACTION_endorse) || (actionName == AppLogRecord.ACTION_interest)) {
            isUseful = true;
        }
        else if ( (actionName == AppLogRecord.ACTION_enter) || (actionName == AppLogRecord.ACTION_leave) ) {
            int actionValue = record.getActionValue();
            if (actionValue == AppLogRecord.ACTION_VAL_merchant) {
                isUseful = true;
            }
            else if (actionValue == AppLogRecord.ACTION_VAL_merchantDetails) {
                isUseful = true;
            }
        }
        else if ((actionName == AppLogRecord.ACTION_bookmark) || (actionName == AppLogRecord.ACTION_unbookmark)) {
            isUseful = true;
        }
        else if (actionName == AppLogRecord.ACTION_add) {
            int actionValue = record.getActionValue();
            if (actionValue == AppLogRecord.ACTION_VAL_merchant) {
                isUseful = true;
            }
        }

        return isUseful;
    }

    @Override
    public boolean appendAppLog(AppLogRecord record) {

        JavaSparkContext sc = null;

        if (SparkContext.getJsscStartFlag()) {
            SparkContext.setJsscStartFlag();
            sc = SparkContext.getJavaStreamingContext().sc();
        }
        else {
            SparkContext.setJscStartFlag();
            sc = SparkContext.getSparkContext();
        }

        AppLogRecord appLogRecord = (AppLogRecord) record;
        List<AppLogRecord> appLogRecordList = new ArrayList<AppLogRecord>();
        appLogRecordList.add(appLogRecord);

        JavaRDD<AppLogRecord> javaRDD = sc.parallelize(appLogRecordList);

        if (mFilter4TrainDataFlag) {
            // debugging starts here
            MaxLogger.debug(UserMerchantFeatureFilter.class,
                    "------------------" + "Append train data [appendInputData] " + "," + getName() + ","
                            + appLogRecord.getActionName() + ";" + appLogRecord.getActionValue() + ":" + appLogRecord.getActionId() + " ------------------");
            // debugging ends here

            if (mAppLogRecords_Train == null) {
                mAppLogRecords_Train = javaRDD;
            } else {
                mAppLogRecords_Train = mAppLogRecords_Train.union(javaRDD);
            }
        }
        else {
            // For debug information
            MaxLogger.debug(UserMerchantFeatureFilter.class,
                    "------------------" + "Append test data [appendInputData] " + "," + getName() + ","
                            + appLogRecord.getActionName() + ";" + appLogRecord.getActionValue() + ":" + appLogRecord.getActionId() + " ------------------");
            //

            if (mAppLogRecords_Test == null) {
                mAppLogRecords_Test = javaRDD;
            } else {
                mAppLogRecords_Test = mAppLogRecords_Test.union(javaRDD);
            }
        }

        return true;
    }

    @Override
    public JavaRDD<AppLogRecord> getInputAppLogs() {

        JavaRDD<AppLogRecord> res = null;

        if (mFilter4TrainDataFlag) {
            if (mAppLogRecords_Train == null) {
                MaxLogger.error(UserMerchantFeatureFilter.class, ErrMsg.ERR_MSG_NullTrainData + ", " + this.getName());
            }

            res = mAppLogRecords_Train;
            mAppLogRecords_Train = null;
        }
        else {
            if (mAppLogRecords_Test == null) {
                MaxLogger.error(UserMerchantFeatureFilter.class, ErrMsg.ERR_MSG_NullTestData + ", " + this.getName());
            }
            res = mAppLogRecords_Test;
            mAppLogRecords_Test = null;
        }

        return res;
    }

    @Override
    public void release() {

        if (mAppLogRecords_Train != null) {
            mAppLogRecords_Train = null;
        }

        if (mAppLogRecords_Test != null) {
            mAppLogRecords_Test = null;
        }
    }
}
