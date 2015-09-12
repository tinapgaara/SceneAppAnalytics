package maxA.cluster;

/**
 * Created by max2 on 7/31/15.
 */
public interface IUserClusterModel {

    public void setClusteringModel(IClusteringModel clusteringModel);
    public IClusteringModel getClusteringModel();

    public IClusterResult filterClusterByUserId(long userId);

    public void cluster(int clusterNum, IClusterData data);

}
