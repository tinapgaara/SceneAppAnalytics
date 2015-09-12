package maxA.userProfile.feature.userMerchant;

import maxA.io.AppLogRecord;
import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/16/15.
 */
public class AskField implements IFeatureField {

    @Override
    public String getName() {
        return UserMerchantFeatureField.ask.getName();
    }

    @Override
    public int getIndex() {
        return UserMerchantFeatureField.ask.getIndex();
    }

    @Override
    public void release() {
    }

}