package maxA.userProfile.impl.linearRegression;

import maxA.userProfile.IFeature;
import maxA.userProfile.IOnlineData;
import maxA.userProfile.IUserProfileModel;
import maxA.userProfile.impl.GenericUserProfileModel;
import maxA.userProfile.impl.GenericUserProfileModelUpdater;

/**
 * Created by TAN on 7/26/2015.
 */
public class LR_UserProfileModelUpdater extends GenericUserProfileModelUpdater {

    private static LR_UserProfileModelUpdater m_instance = null;

    public static LR_UserProfileModelUpdater getInstance(IUserProfileModel userProfileModel) {

        if (m_instance == null) {
            m_instance = new LR_UserProfileModelUpdater(userProfileModel);// match LR_UserProfileModelUpdater to LR_UserProfileModel
        }
        return m_instance;
    }

    private LR_UserProfileModelUpdater(IUserProfileModel userProfileModel) {
        super(userProfileModel);
    }
}
