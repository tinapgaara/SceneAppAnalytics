package maxA.userProfile.impl;

import maxA.userProfile.IFeatureField;
import maxA.userProfile.IFeatureVector;
import maxA.userProfile.feature.userMerchant.UserMerchantFeature;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.Serializable;
import java.util.List;

/**
 * Created by max2 on 7/23/15.
 */
public abstract class GenericFeatureVector implements IFeatureVector, Serializable {

    protected double[] mFieldValues;


    protected abstract int getLength();

    public GenericFeatureVector() {

        mFieldValues = new double[getLength()];
    }

    public double[] getFieldValues() {

        return mFieldValues;
    }

    public void setFieldValue(IFeatureField field, double value) {

        mFieldValues[field.getIndex()] = value;
    }

    @Override
    public Vector getData() {

        if (mFieldValues == null) {
            return null;
        }

        Vector featureVector = null;
        int length = mFieldValues.length;

        int numNonZeroValues = 0;
        for (int i = 0; i < length; i++) {
            if (mFieldValues[i] != 0) {
                numNonZeroValues++;
            }
        }

        int[] indices = new int[numNonZeroValues];
        double[] values = new double[numNonZeroValues];
        int j = 0;
        for (int i = 0; i < length; i++) {
            if (mFieldValues[i] != 0) {
                indices[j] = i;
                values[j] = mFieldValues[i];
                j++;
            }
        }

        featureVector = Vectors.sparse(length, indices, values);

        return featureVector;
    }

    protected void combineFieldValues(GenericFeatureVector other) {

        if (other != null) {
            for (int i = 0; i < mFieldValues.length; i++) {
                mFieldValues[i] += other.mFieldValues[i];
            }
        }
    }

    protected double[] copyFieldValues() {

        int length = mFieldValues.length;

        double[] newFieldValues = new double[length];
        for (int i = 0; i < length; i++) {
            newFieldValues[i] = mFieldValues[i];
        }

        return newFieldValues;
    }

    @Override
    public void release() {

        mFieldValues = null;
    }
}
