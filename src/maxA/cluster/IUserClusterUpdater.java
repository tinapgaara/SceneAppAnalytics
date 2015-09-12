package maxA.cluster;

import maxA.userProfile.IUserProfileModel;

/**
 * Created by max2 on 7/30/15.
 */
public interface IUserClusterUpdater {

    public void setUserClusterModel(IUserClusterModel userClusterModel);
    public IUserClusterModel getUserClusterModel();

    public void setUserProfileModel(IUserProfileModel userProfileModel);
    public IUserProfileModel getUserProfileModel();

    public void updateClusters(int clusterNum);

}
