package maxA.userProfile;

import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;

import java.util.Map;
import java.util.Set;

/**
 * Created by TAN on 6/15/2015.
 */
public interface IUserProfileModel {

	/*
	public int addUser(User user);		// return user Id
	public void removeUser(int userId);
	//*/

	public void addUserAttributeValue(long userId, String attributeName, UserAttributeValue attribute);
	public UserAttributeValue getUserAttributeValue(long userId, String attributeName);

	public void registerFeature(IFeature feature);
	public void unregisterFeature(IFeature feature);
	/*
	public int registerFeature(IFeature feature);	// return feature unique id
	public void unregisterFeature(int featureId);
	//*/

	public void trainFeatureModel(IFeature feature, ITrainData trainData)
			throws UserProfileException;
	/*
	public void trainFeatureModel(int featureId, ITrainData trainData)
			throws UserProfileException;
	//*/

	public IUserProfile getUserProfile(long userId);

	public CoordinateMatrix getFeatureMatrix(String featureName);
	/*
	public CoordinateMatrix getFeatureMatrix(int featureId);
	//*/
	// auxiliary methods:
	public Set<IFeature> getAllFeatures();
	/*
	public IFeature[] getAllFeatures();
	public int getFeatureIdByName(String featureName);
	//*/

	public void release();
}

