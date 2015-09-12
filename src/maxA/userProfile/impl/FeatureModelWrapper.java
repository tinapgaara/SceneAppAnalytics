package maxA.userProfile.impl;

import java.io.Serializable;

/**
 * Created by TAN on 7/22/2015.
 */
public class FeatureModelWrapper implements Serializable {

    private Object mFeatureModel;

    public FeatureModelWrapper(Object featureModel) {
        mFeatureModel = featureModel;
    }

    public Object getFeatureModel() {
        return mFeatureModel;
    }

    public void release() {
        mFeatureModel = null;
    }
}
