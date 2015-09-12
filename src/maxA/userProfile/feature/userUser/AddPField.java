package maxA.userProfile.feature.userUser;

import maxA.userProfile.IFeatureField;
import scala.tools.nsc.backend.icode.Members;

/**
 * Created by max2 on 7/24/15.
 */
public class AddPField implements IFeatureField {

    @Override
    public String getName() { return UserUserFeatureField.addP.getName(); }

    @Override
    public int getIndex() { return UserUserFeatureField.addP.getIndex(); }

    @Override
    public void release() {

    }


}
