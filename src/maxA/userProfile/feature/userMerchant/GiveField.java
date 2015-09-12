package maxA.userProfile.feature.userMerchant;

import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/16/15.
 */
public class GiveField implements IFeatureField {

    @Override
    public String getName() {
        return UserMerchantFeatureField.give.getName();
    }

    @Override
    public int getIndex() {
        return UserMerchantFeatureField.give.getIndex();
    }

    @Override
    public void release() {
    }

}

