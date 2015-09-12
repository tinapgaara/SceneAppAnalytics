package maxA.userProfile.feature.userUser;

import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/24/15.
 */
public enum UserUserFeatureField implements IFeatureField {

    endorse(0, "endorse"), addP(1, "addP"), interest(2, "interest");

    private int mIndex;
    private String mName;

    private UserUserFeatureField (int index, String name) {

        mIndex = index;
        mName = name;
    }

    public int getIndex() { return mIndex; }

    public String getName() { return mName; }

    public void release() {
        mName = null;
    }

}
