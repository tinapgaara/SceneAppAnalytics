package maxA.userProfile.feature.userUser;

import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/24/15.
 */
public class EndorseField implements IFeatureField {

    @Override
    public String getName() { return UserUserFeatureField.endorse.getName(); }

    @Override
    public int getIndex() { return UserUserFeatureField.endorse.getIndex(); }

    @Override
    public void release() {
    }
}
