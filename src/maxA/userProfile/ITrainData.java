package maxA.userProfile;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Created by TAN on 7/5/2015.
 */
public interface ITrainData extends java.io.Serializable {

    public JavaRDD<LabeledPoint> getData();

    public void release();

}
