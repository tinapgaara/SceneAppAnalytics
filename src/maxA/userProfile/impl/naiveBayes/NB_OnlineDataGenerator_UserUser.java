package maxA.userProfile.impl.naiveBayes;

import maxA.common.redis.RedisHelper;
import maxA.io.AppLogRecord;
import maxA.userProfile.IOnlineData;
import maxA.userProfile.IOnlineDataEntry;
import maxA.userProfile.feature.userUser.UserUserFeatureField;
import maxA.userProfile.impl.GenericFeatureVector_UserUser;
import maxA.userProfile.impl.GenericOnlineDataGenerator_UserUser;
import maxA.userProfile.impl.UnifiedOnlineData;
import maxA.userProfile.impl.UnifiedOnlineDataEntry;
import maxA.util.MaxLogger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by max2 on 8/10/15.
 */
public class NB_OnlineDataGenerator_UserUser extends GenericOnlineDataGenerator_UserUser {

    private static NB_OnlineDataGenerator_UserUser m_instance = null;

    public static NB_OnlineDataGenerator_UserUser getInstance() {

        if (m_instance == null) {
            m_instance = new NB_OnlineDataGenerator_UserUser();
        }
        return m_instance;
    }

    private NB_OnlineDataGenerator_UserUser() {
        // nothing to do here
    }

    protected GenericFeatureVector_UserUser createFeatureVector_UserUser() {
        return new NB_FeatureVector_UserUser();
    }
}