package maxA.cluster;

/**
 * Created by max2 on 7/30/15.
 */
public interface IClusteringModel {

    public String getName();

    public IClusterResult cluster(int clusterNum, IClusterData clusterData);

}
