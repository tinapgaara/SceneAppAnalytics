package maxA.userProfile.impl.naiveBayes;

import maxA.userProfile.impl.GenericUserProfile;

/**
 * Created by max2 on 7/22/15.
 */
public class NB_UserProfile extends GenericUserProfile {

    private NB_UserProfileModel mModel;

    public NB_UserProfile(Long userID, NB_UserProfileModel model) {

        super(userID, model);
    }
}
