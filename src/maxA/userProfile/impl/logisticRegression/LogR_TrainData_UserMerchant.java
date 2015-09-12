package maxA.userProfile.impl.logisticRegression;

import maxA.userProfile.ITrainData;
import maxA.userProfile.impl.GenericTrainData_UserMerchant;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Created by TAN on 7/22/2015.
 */
public class LogR_TrainData_UserMerchant extends GenericTrainData_UserMerchant {

    public LogR_TrainData_UserMerchant(JavaRDD<LabeledPoint> data) {

        super(data);

        // debugging starts here
        MaxLogger.debug(LogR_TrainData_UserMerchant.class, "----------------[Set Train Data]----------------");
        // debugging ends here
    }
}
