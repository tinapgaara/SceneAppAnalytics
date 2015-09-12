package maxA.userProfile.feature.userMerchant;

import maxA.userProfile.IFeatureField;

/**
 * Created by max2 on 7/16/15.
 */
public class ReplyField implements IFeatureField {

    @Override
    public String getName() {
        return UserMerchantFeatureField.reply.getName();
    }

    @Override
    public int getIndex() {
        return UserMerchantFeatureField.reply.getIndex();
    }

    @Override
    public void release() {
    }

}
