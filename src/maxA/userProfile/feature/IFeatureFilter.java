package maxA.userProfile.feature;

import maxA.io.IRecord;

import java.util.List;

/**
 * Created by TAN on 7/6/2015.
 */
public interface IFeatureFilter {

    public String getName();

    public boolean isUseful(IRecord record);
}
