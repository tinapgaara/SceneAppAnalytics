package maxA.cluster.impl;

import maxA.cluster.IUserClusterModel;
import maxA.cluster.IUserClusterUpdater;
import maxA.userProfile.IUserProfileModel;
import maxA.userProfile.feature.userUser.UserUserFeature;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;

import java.io.Serializable;

/**
 * Created by max2 on 7/30/15.
 */
public abstract class GenericUserClusterUpdater implements IUserClusterUpdater, Serializable {

    protected IUserClusterModel mUserClusterModel;
    protected IUserProfileModel mUserProfileModel;

    protected abstract void updateClustersByModel(int clusterNum, CoordinateMatrix userUserMatrix);

    protected GenericUserClusterUpdater(IUserClusterModel userClusterModel) {

        mUserClusterModel = userClusterModel;
    }

    @Override
    public void setUserClusterModel(IUserClusterModel userClusterModel) { mUserClusterModel = userClusterModel; }

    @Override
    public IUserClusterModel getUserClusterModel() {

        return mUserClusterModel;
    }

    @Override
    public void setUserProfileModel(IUserProfileModel userProfileModel) {

        mUserProfileModel = userProfileModel;
    }

    public IUserProfileModel getUserProfileModel() {

        return mUserProfileModel;
    }

    @Override
    public void updateClusters(int clusterNum) {

        if (clusterNum < 1) {
            MaxLogger.error(GenericUserClusterUpdater.class,
                            ErrMsg.msg(ErrMsg.ERR_MSG_UnknownClusterNum, clusterNum+""));

            return;
        }

        if (mUserProfileModel == null) {
            MaxLogger.error(GenericUserClusterUpdater.class,
                            ErrMsg.ERR_MSG_NullUserProfileModel);

            return;
        }

        if (mUserClusterModel == null) {
            MaxLogger.error(GenericUserClusterUpdater.class,
                            ErrMsg.ERR_MSG_NullClusterModel);
            return;
        }

        /*
        int featureId = mUserProfileModel.getFeatureIdByName(UserUserFeature.FEATURE_NAME); // For test: UserMerchantFeature.FEATURE_NAME
        if (featureId == 0) {
            MaxLogger.error(GenericUserClusterUpdater.class,
                            ErrMsg.ERR_MSG_UnregisteredFeature);
        }
        //*/

        CoordinateMatrix matrix =  mUserProfileModel.getFeatureMatrix(UserUserFeature.FEATURE_NAME);
        if (matrix == null) {
            MaxLogger.error(GenericUserClusterUpdater.class,
                            ErrMsg.ERR_MSG_NullMatrix);
            return;
        }

        updateClustersByModel(clusterNum, matrix);
    }
}
