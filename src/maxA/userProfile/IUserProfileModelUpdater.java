package maxA.userProfile;

/**
 * Created by TAN on 7/25/2015.
 */
public interface IUserProfileModelUpdater {

    public IUserProfileModel getUserProfileModel();

    public void setOnlineDataGenerator(IOnlineDataGenerator tdg);
    public IOnlineDataGenerator getOnlineDataGenerator();

    /*
    public void updateModel(int featureId, IOnlineData testData);
   //*/
    public void updateModel(IFeature feature, IOnlineData testData);
}
