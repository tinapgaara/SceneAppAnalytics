package maxA.userProfile.feature.userMerchant;

import maxA.userProfile.IFeatureField;

/**
 * Created by TAN on 7/15/2015.
 */

public enum UserMerchantFeatureField implements IFeatureField {

    give(0, "give"), reply(1, "reply"), ask(2, "ask"), enterM(3, "enterM"), enterDetails(4,"enterDetails"),
    interest(5, "interest"), addM(6, "addM");

    private int mIndex;
    private String mName;


    private UserMerchantFeatureField(int index, String name) { // each enumeration has two fields: index and name

        mIndex = index;
        mName = name;
    }

    @Override
    public int getIndex() {
        return mIndex;
    }

    @Override
    public String getName() {
        return mName;
    }

    @Override
    public void release() {
        mName = null;
    }

}
