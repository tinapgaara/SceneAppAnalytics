package maxA.cluster.impl.PIC;

import maxA.cluster.IClusterResult;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.PowerIterationClustering;

/**
 * Created by max2 on 8/1/15.
 */
public class PIC_ClusterResult implements IClusterResult<PowerIterationClustering.Assignment> {

    private JavaRDD<PowerIterationClustering.Assignment> mGroups;

    public PIC_ClusterResult() {
        mGroups = null;
    }

    public PIC_ClusterResult(JavaRDD<PowerIterationClustering.Assignment> groups) {
        mGroups = groups;
    }

    @Override
    public void setResult(JavaRDD<PowerIterationClustering.Assignment> groups) {
        mGroups = groups;
    }

    @Override
    public JavaRDD<PowerIterationClustering.Assignment> getResult() {
        return mGroups;
    }

    @Override
    public void release() {
        mGroups = null;
    }

}
