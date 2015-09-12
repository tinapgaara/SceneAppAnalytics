package maxA.userProfile;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

/**
 * Created by max2 on 7/26/15.
 */
public interface IOnlineData extends java.io.Serializable {

    public JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> getData();

    public void release();

}
