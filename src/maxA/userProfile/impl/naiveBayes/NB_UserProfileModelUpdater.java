package maxA.userProfile.impl.naiveBayes;

import maxA.userProfile.IFeature;
import maxA.userProfile.IOnlineData;
import maxA.userProfile.IUserProfileModel;
import maxA.userProfile.impl.GenericUserProfileModel;
import maxA.userProfile.impl.GenericUserProfileModelUpdater;

/**
 * Created by max2 on 8/10/15.
 */
public class NB_UserProfileModelUpdater extends GenericUserProfileModelUpdater {

    private static NB_UserProfileModelUpdater m_instance = null;

    public static NB_UserProfileModelUpdater getInstance(IUserProfileModel userProfileModel) {

        if (m_instance == null) {
            m_instance = new NB_UserProfileModelUpdater(userProfileModel);
        }
        return m_instance;
    }

    private NB_UserProfileModelUpdater(IUserProfileModel userProfileModel) {
        super(userProfileModel);
    }
}
