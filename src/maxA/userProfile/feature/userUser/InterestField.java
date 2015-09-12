package maxA.userProfile.feature.userUser;

import maxA.userProfile.IFeatureField;
import maxA.userProfile.feature.userUser.UserUserFeatureField;

/**
 * Created by max2 on 7/24/15.
 */
public class InterestField implements IFeatureField {

    @Override
    public String getName() {
        return UserUserFeatureField.interest.getName();
    }

    @Override
    public int getIndex() {
        return UserUserFeatureField.interest.getIndex();
    }

    @Override
    public void release() {
    }

}
