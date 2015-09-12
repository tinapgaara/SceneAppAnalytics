package maxA.cluster.impl.PIC;

import maxA.cluster.IClusterData;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple3;

/**
 * Created by max2 on 8/1/15.
 */
public class PIC_ClusterData implements IClusterData<Tuple3<Long, Long, Double>> {

    private JavaRDD<Tuple3<Long, Long, Double>> mClusterData;

    public PIC_ClusterData() {
        mClusterData = null;
    }

    public PIC_ClusterData(JavaRDD<Tuple3<Long, Long, Double>> clusterData) {
        mClusterData = clusterData;
    }

    @Override
    public void setData(JavaRDD<Tuple3<Long, Long, Double>> clusterData) {
        mClusterData = clusterData;
    }

    @Override
    public JavaRDD<Tuple3<Long, Long, Double>> getData() {
        return mClusterData;
    }

    @Override
    public void release() {
        mClusterData = null;
    }
}
