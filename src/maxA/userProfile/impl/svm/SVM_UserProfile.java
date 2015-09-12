package maxA.userProfile.impl.svm;

import maxA.userProfile.impl.GenericUserProfile;

/**
 * Created by max2 on 7/22/15.
 */
public class SVM_UserProfile extends GenericUserProfile {

    private SVM_UserProfileModel mModel;

    public SVM_UserProfile(Long userID, SVM_UserProfileModel model) {

        super(userID, model);
    }

}
