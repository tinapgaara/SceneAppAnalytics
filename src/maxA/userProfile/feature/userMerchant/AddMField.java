package maxA.userProfile.feature.userMerchant;

import maxA.io.AppLogRecord;
import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/16/15.
 */
public class AddMField implements IFeatureField {

    @Override
    public String getName() {
        return UserMerchantFeatureField.addM.getName();
    }

    @Override
    public int getIndex() {
        return UserMerchantFeatureField.addM.getIndex();
    }

    @Override
    public void release() {
    }

}
