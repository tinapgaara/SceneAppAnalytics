package maxA.userProfile.impl;

import maxA.userProfile.*;

import java.io.Serializable;

/**
 * Created by TAN on 7/27/2015.
 */
public abstract class GenericUserProfileModelUpdater implements IUserProfileModelUpdater, Serializable {

    protected IUserProfileModel mUserProfileModel;
    protected IOnlineDataGenerator mOnlineDataGenerator;

    protected GenericUserProfileModelUpdater(IUserProfileModel userProfileModel) {

        mUserProfileModel = userProfileModel;
    }

    @Override
    public IUserProfileModel getUserProfileModel() {

        return mUserProfileModel;
    }

    @Override
    public void setOnlineDataGenerator(IOnlineDataGenerator tdg) {

        mOnlineDataGenerator = tdg;
    }

    @Override
    public IOnlineDataGenerator getOnlineDataGenerator() {

        return mOnlineDataGenerator;
    }

    @Override
    public void updateModel(IFeature feature, IOnlineData testData) {

        GenericUserProfileModel userProfileModel = (GenericUserProfileModel)this.getUserProfileModel();
        userProfileModel.updateModelByOnlineData(feature, testData);
    }
}
