
package maxA.recommender;

import maxA.cluster.IUserClusterModel;
import maxA.userProfile.IUserProfileModel;

import java.util.List;

/**
 * Created by TAN on 2015/6/15.
 */
public interface IRecommender {

	public IUserClusterModel getUserClusterModel();
	public void setUserClusterModel(IUserClusterModel userClusterModel);

	public IUserProfileModel getUserProfileModel();
	public void setUserProfileModel(IUserProfileModel userProfileModel);

	public List<IMerchantRecommendation> recMerchants2User(long userId,
		   int maxNumOfMerchants);

	/*
	public List<IUserRecommendation> recTopUsersForMerchant(
			String merchantId, int maxNumOfUsers,
			IContext context,
			List<IUserRecommendation> prevRecommendations);


	public List<IMerchantRecommendation> recMerchants2User(String userId,
			int maxNumOfMerchants,
			IContext context,
			List<IMerchantRecommendation> prevRecommendations);
	*/
	
}

