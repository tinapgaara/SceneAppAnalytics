package maxA.userProfile;

import org.apache.spark.mllib.linalg.Vector;

/**
 * Created by TAN on 7/5/2015.
 */
public interface IFeatureVector {

    public Vector getData();

    public void release();

}
