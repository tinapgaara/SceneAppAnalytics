package maxA.userProfile.feature;

import maxA.userProfile.IFeature;
import maxA.userProfile.IFeatureField;
import maxA.userProfile.IFeatureFilter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by TAN on 7/5/2015.
 */
public abstract class GenericFeature implements IFeature, Serializable {

    protected String mName;

    protected Map<Integer, IFeatureField> mFields;

    protected IFeatureFilter mFeatureFilter;

    protected GenericFeature(String name) {

        mName = name;
        mFields = null;
        mFeatureFilter = null;
    }

    @Override
    public String getName() {

        return mName;
    }

    @Override
    public int registerField(IFeatureField field) {

        if (field == null) {
            return 0;
        }

        if (mFields == null) {
            mFields = new HashMap<Integer, IFeatureField>();
        }

        int fieldId = field.getIndex();
        mFields.put(new Integer(fieldId), field);

        return fieldId;
    }

    @Override
    public void unregisterField(int filedId) {

        if (mFields != null) {
            mFields.remove(new Integer(filedId));
        }
    }

    @Override
    public void setFeatureFilter(IFeatureFilter featureFilter) {
        mFeatureFilter = featureFilter;
    }

    @Override
    public IFeatureFilter getFeatureFilter() {
        return mFeatureFilter;
    }

    @Override
    public void release() {

        if (mFields != null) {
            mFields.clear();
            mFields = null;
        }
    }
}
