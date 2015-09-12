package maxA.userProfile.impl.svm;

import maxA.userProfile.IFeature;
import maxA.userProfile.IOnlineData;
import maxA.userProfile.IUserProfileModel;
import maxA.userProfile.impl.GenericUserProfileModel;
import maxA.userProfile.impl.GenericUserProfileModelUpdater;

/**
 * Created by max2 on 8/10/15.
 */
public class SVM_UserProfileModelUpdater extends GenericUserProfileModelUpdater {

    private static SVM_UserProfileModelUpdater m_instance = null;

    public static SVM_UserProfileModelUpdater getInstance(IUserProfileModel userProfileModel) {

        if (m_instance == null) {
            m_instance = new SVM_UserProfileModelUpdater(userProfileModel);
        }
        return m_instance;
    }

    private SVM_UserProfileModelUpdater(IUserProfileModel userProfileModel) {
        super(userProfileModel);
    }
}

