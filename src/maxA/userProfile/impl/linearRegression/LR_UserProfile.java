package maxA.userProfile.impl.linearRegression;

import maxA.userProfile.*;
import maxA.userProfile.impl.GenericUserProfile;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;

import java.util.*;

/**
 * Created by TAN on 7/5/2015.
 */
public class LR_UserProfile extends GenericUserProfile {

    public LR_UserProfile(Long userID, LR_UserProfileModel model) {

        super(userID, model);
    }

}
