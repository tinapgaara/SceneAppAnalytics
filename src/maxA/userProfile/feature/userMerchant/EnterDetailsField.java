package maxA.userProfile.feature.userMerchant;

import maxA.io.AppLogRecord;
import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/19/15.
 */
public class EnterDetailsField implements IFeatureField {

    @Override
    public String getName() {
        return UserMerchantFeatureField.enterDetails.getName();
    }

    @Override
    public int getIndex() {
        return UserMerchantFeatureField.enterDetails.getIndex();
    }

    @Override
    public void release() {
    }
}
