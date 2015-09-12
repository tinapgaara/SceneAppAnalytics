package maxA.cluster.impl;

import maxA.cluster.IClusterData;
import maxA.cluster.IClusteringModel;
import maxA.cluster.IUserClusterModel;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import java.io.Serializable;

/**
 * Created by max2 on 7/31/15.
 */
public abstract class GenericUserClusterModel implements IUserClusterModel, Serializable {

    protected IClusteringModel mClusteringModel;

    protected GenericUserClusterModel(IClusteringModel clusteringModel) {

        mClusteringModel = clusteringModel;
    }

    @Override
    public void setClusteringModel(IClusteringModel clusteringModel) { mClusteringModel = clusteringModel; }

    @Override
    public IClusteringModel getClusteringModel() {

        return mClusteringModel;
    }

}
