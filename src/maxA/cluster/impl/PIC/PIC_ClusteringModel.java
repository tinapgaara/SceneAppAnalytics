package maxA.cluster.impl.PIC;

import maxA.cluster.IClusterData;
import maxA.cluster.IClusterResult;
import maxA.cluster.IClusteringModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import scala.Tuple3;

import java.io.Serializable;

/**
 * Created by max2 on 7/30/15.
 */
public class PIC_ClusteringModel implements IClusteringModel, Serializable {

    private static final String NAME = "PIC";

    private static PIC_ClusteringModel m_instance = null;

    private PowerIterationClustering mClustering;

    public static PIC_ClusteringModel getInstance() {

        if (m_instance == null) {
            m_instance = new PIC_ClusteringModel();
        }
        return m_instance;
    }

    private PIC_ClusteringModel() {
        mClustering = new PowerIterationClustering();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public IClusterResult cluster(int clusterNum, IClusterData clusterData) {

        mClustering.setK(clusterNum);

        JavaRDD<Tuple3<Long, Long, Double>> picClusterData = ( (PIC_ClusterData) clusterData).getData();
        PowerIterationClusteringModel clusterModel = mClustering.run(picClusterData);
        JavaRDD<PowerIterationClustering.Assignment> groups = clusterModel.assignments().toJavaRDD();

        return new PIC_ClusterResult(groups);
    }

}
