package maxA.userProfile.feature.userMerchant;

import maxA.io.AppLogRecord;
import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/16/15.
 */
public class EnterMField implements IFeatureField {

    @Override
    public String getName() {
        return UserMerchantFeatureField.enterM.getName();
    }

    @Override
    public int getIndex() {
        return UserMerchantFeatureField.enterM.getIndex();
    }

    @Override
    public void release() {
    }

}
