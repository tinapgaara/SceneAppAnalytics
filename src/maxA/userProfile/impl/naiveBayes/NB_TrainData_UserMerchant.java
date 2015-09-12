package maxA.userProfile.impl.naiveBayes;

import maxA.userProfile.impl.GenericTrainData_UserMerchant;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Created by TAN on 7/22/2015.
 */
public class NB_TrainData_UserMerchant extends GenericTrainData_UserMerchant {

    public NB_TrainData_UserMerchant(JavaRDD<LabeledPoint> data) {

        super(data);

        // debugging starts here
        MaxLogger.debug(NB_TrainData_UserMerchant.class, "----------------[Set Train Data]----------------");
        // debugging ends here
    }
}
