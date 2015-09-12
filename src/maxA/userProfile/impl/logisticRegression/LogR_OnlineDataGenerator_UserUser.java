package maxA.userProfile.impl.logisticRegression;

import maxA.userProfile.impl.GenericFeatureVector_UserUser;
import maxA.userProfile.impl.GenericOnlineDataGenerator_UserUser;
/**
 * Created by max2 on 8/10/15.
 */
public class LogR_OnlineDataGenerator_UserUser extends GenericOnlineDataGenerator_UserUser {

    private static LogR_OnlineDataGenerator_UserUser m_instance = null;

    public static LogR_OnlineDataGenerator_UserUser getInstance() {

        if (m_instance == null) {
            m_instance = new LogR_OnlineDataGenerator_UserUser();
        }
        return m_instance;
    }

    private LogR_OnlineDataGenerator_UserUser() {
        // nothing to do here
    }

    protected GenericFeatureVector_UserUser createFeatureVector_UserUser() {
        return new LogR_FeatureVector_UserUser();
    }

}
