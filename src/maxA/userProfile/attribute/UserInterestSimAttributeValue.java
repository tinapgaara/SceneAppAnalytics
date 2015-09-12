package maxA.userProfile.attribute;

import maxA.userProfile.UserAttributeValue;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by TAN on 7/29/2015.
 */
public class UserInterestSimAttributeValue extends UserAttributeValue {

    private static Map<Long, Double> mUserInterestSim;

    public UserInterestSimAttributeValue() {

        mUserInterestSim = null;
    }

    public Map<Long, Double> getUserInterestSim() {
        return mUserInterestSim;
    }

    /*
    public void setUserInterestVector(Map<Long, Double> interestSim) {
        mUserInterestSim = interestSim;
    }
    //*/

    public boolean ifContainsSimWithOtherUser(long otherUserId) {

        if ( (mUserInterestSim == null) || mUserInterestSim.isEmpty() ) {

            return false;
        }
        else if ( ! mUserInterestSim.containsKey(new Long(otherUserId)) ) {

            return false;
        }
        // debugging starts here
        MaxLogger.debug(UserInterestSimAttributeValue.class,
                        "--------------[ifContainsUserSim] YES " );
        // debugging ends here

        return true;
    }

    public void addInterestSimWithOtherUser(long otherUserId, double sim) {

        if (mUserInterestSim == null) {
            mUserInterestSim = new HashMap<Long, Double>();
        }
        mUserInterestSim.put(new Long(otherUserId), sim);
    }

    public double getSimWithOtherUser(long otherUserId) {

        if (mUserInterestSim == null) {
            MaxLogger.info(UserInterestSimAttributeValue.class,
                           ErrMsg.ERR_MSG_NULLInterestRow);

            return 0;
        }

        return mUserInterestSim.get(new Long(otherUserId));
    }

    public void release() {

        if (mUserInterestSim != null) {
            mUserInterestSim.clear();
            mUserInterestSim = null;
        }
    }
}
