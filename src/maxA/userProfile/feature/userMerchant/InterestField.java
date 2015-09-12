package maxA.userProfile.feature.userMerchant;

import maxA.io.AppLogRecord;
import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/16/15.
 */
public class InterestField implements IFeatureField {

    @Override
    public String getName() {
        return UserMerchantFeatureField.interest.getName();
    }

    @Override
    public int getIndex() {
        return UserMerchantFeatureField.interest.getIndex();
    }

    @Override
    public void release() {
    }

}
