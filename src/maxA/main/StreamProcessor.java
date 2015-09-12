package maxA.main;

import maxA.cluster.IUserClusterUpdater;
import maxA.io.AppLogRecord;
import maxA.userProfile.*;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by max2 on 7/25/2015.
 */
public class StreamProcessor implements Serializable {

    private static final int TIME_WINDOW_LENGTH_InMilliseconds = 1;

    private static StreamProcessor m_instance = null;

    private IUserProfileModelUpdater mUserProfileModelUpdater;
    private IUserClusterUpdater mUserClusterUpdater;

    private long mLastUserClusterUpdateTime;

    private StreamProcessor() {

        mUserProfileModelUpdater = null;
        mUserClusterUpdater = null;

        mLastUserClusterUpdateTime = 0;
    }

    public static StreamProcessor getInstance() {

        if (m_instance == null) {
            m_instance = new StreamProcessor();
        }
        return m_instance;
    }

    public void setUserProfileModelUpdater(IUserProfileModelUpdater userProfileModelUpdater) {

        mUserProfileModelUpdater = userProfileModelUpdater;
    }

    public void setUserClusterUpdater(IUserClusterUpdater userClusterUpdater) {

        mUserClusterUpdater = userClusterUpdater;
    }

    public void process() {

        if (mUserProfileModelUpdater != null) {

            IUserProfileModel userProfileModel = mUserProfileModelUpdater.getUserProfileModel();
            IOnlineDataGenerator onlineDataGenerator = mUserProfileModelUpdater.getOnlineDataGenerator();
            MaxLogger.info(StreamProcessor.class,
                           "----------------[1. trigger userProfile updater] ----------------");

            /*
            IFeature[] features = userProfileModel.getAllFeatures(); // get all features which have been registered when building the model
            //*/
            Set<IFeature> features = userProfileModel.getAllFeatures();

            IFeatureFilter featureFilter = null;
            int featureId;

            for (IFeature feature : features) {
                featureFilter = feature.getFeatureFilter();

                // For debug information
                MaxLogger.debug(StreamProcessor.class,
                                "----------------[process]:" + featureFilter.getName() + "----------------");
                //

                /*
                featureId = userProfileModel.getFeatureIdByName(feature.getName());
                //*/
                JavaRDD<AppLogRecord> streamAppRDD = featureFilter.getInputAppLogs();

                if (streamAppRDD != null) {
                    IOnlineData onlineData = onlineDataGenerator.generateOnlineDataByAppLogs(feature, streamAppRDD);
                    /*
                    mUserProfileModelUpdater.updateModel(featureId, onlineData);
                    //*/
                    mUserProfileModelUpdater.updateModel(feature, onlineData);
                    featureFilter.release();
                }
                else {
                    MaxLogger.error(StreamProcessor.class, ErrMsg.ERR_MSG_NullOnlineData);
                }
            }
        }
        else {
            MaxLogger.info(StreamProcessor.class, ErrMsg.ERR_MSG_NullUserProfileModelUpdater);
            return;
        }

        if (mUserClusterUpdater != null) {

            int clusterNum = 3; // For test
            MaxLogger.info(StreamProcessor.class,
                            "----------------[2. trigger userCluster updater] ----------------");

            long curTime = System.currentTimeMillis();

            if (mLastUserClusterUpdateTime == 0) {
                // For debug information
                MaxLogger.debug(StreamProcessor.class,
                                "----------------[first time trigger userCluster updater] ----------------");
                //

                mLastUserClusterUpdateTime = curTime;
            }

            else if (curTime - mLastUserClusterUpdateTime >= TIME_WINDOW_LENGTH_InMilliseconds) {

                mUserClusterUpdater.updateClusters(clusterNum);
                mLastUserClusterUpdateTime = curTime;
            }
        }
        else {
            MaxLogger.info(StreamProcessor.class, ErrMsg.ERR_MSG_NullClusterModelUpdater);
            return;
        }
    }
}
