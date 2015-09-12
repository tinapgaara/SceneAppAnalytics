package maxA.userProfile;

/**
 * Created by TAN on 7/5/2015.
 */
public interface IFeature {

    public String getName();

    public int registerField(IFeatureField field);   // return field's unique id
    public void unregisterField(int filedId);

    public IFeatureFilter getFeatureFilter();
    public void setFeatureFilter(IFeatureFilter featureFilter);

    public void release();

}
