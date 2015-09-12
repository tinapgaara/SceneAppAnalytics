package maxA.userProfile.impl.logisticRegression;

import maxA.userProfile.impl.GenericUserProfile;

/**
 * Created by max2 on 7/22/15.
 */
public class LogR_UserProfile extends GenericUserProfile {

    private LogR_UserProfileModel mModel;

    public LogR_UserProfile(Long userID, LogR_UserProfileModel model) {

        super(userID, model);
    }
}
