package maxA.userProfile.impl.logisticRegression;

import maxA.userProfile.IFeature;
import maxA.userProfile.IOnlineData;
import maxA.userProfile.IUserProfileModel;
import maxA.userProfile.impl.GenericUserProfileModel;
import maxA.userProfile.impl.GenericUserProfileModelUpdater;

/**
 * Created by max2 on 8/10/15.
 */
public class LogR_UserProfileModelUpdater extends GenericUserProfileModelUpdater {

    private static LogR_UserProfileModelUpdater m_instance = null;

    public static LogR_UserProfileModelUpdater getInstance(IUserProfileModel userProfileModel) {

        if (m_instance == null) {
            m_instance = new LogR_UserProfileModelUpdater(userProfileModel);
        }
        return m_instance;
    }

    private LogR_UserProfileModelUpdater(IUserProfileModel userProfileModel) {
        super(userProfileModel);
    }
}
