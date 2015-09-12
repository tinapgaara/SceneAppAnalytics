package maxA.cluster.impl.PIC;

import maxA.cluster.IClusterData;
import maxA.cluster.IClusterResult;
import maxA.cluster.IClusteringModel;
import maxA.cluster.impl.GenericUserClusterModel;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.PowerIterationClustering;

import java.util.List;

/**
 * Created by max2 on 7/31/15.
 */
public class PIC_UserClusterModel extends GenericUserClusterModel {

    private JavaRDD<PowerIterationClustering.Assignment> mGroups;

    public PIC_UserClusterModel(IClusteringModel clusterModel) {

        super(clusterModel);
        mGroups = null;
    }

    private synchronized void setGroups(PIC_ClusterResult groups) {

        mGroups = groups.getResult();
    }

    @Override
    public void cluster(int clusterNum, IClusterData data) {

        if (data == null) {
            MaxLogger.error(GenericUserClusterModel.class,
                            ErrMsg.ERR_MSG_NullMatrix);
            return;
        }

        PIC_ClusterResult clusterResult = (PIC_ClusterResult) mClusteringModel.cluster(clusterNum, data);
        setGroups(clusterResult);

        // debugging starts here
        List<PowerIterationClustering.Assignment> reslist =  mGroups.collect();
        for (PowerIterationClustering.Assignment assign : reslist) {
            MaxLogger.debug(PIC_UserClusterModel.class,
                            "----------------[clusterByModel], [clusters" + assign.cluster() + "," + assign.id()+"]" );
        }
        // debugging ends here
    }

    @Override
    public synchronized IClusterResult filterClusterByUserId(long userId) {

        if (mGroups == null) {
            MaxLogger.error(PIC_UserClusterModel.class, ErrMsg.ERR_MSG_NullClusterAssignment);
            return null;
        }

        final long curUserId = userId;
        JavaRDD<PowerIterationClustering.Assignment> userAssignments = mGroups.filter(
                new Function<PowerIterationClustering.Assignment, Boolean>() {
                    public Boolean call(PowerIterationClustering.Assignment assignment) throws Exception {
                        if (assignment.id() == curUserId) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

        List<PowerIterationClustering.Assignment> userAssignmentsList = userAssignments.collect();

        int clusterId = -1;
        if ( (userAssignments != null) && (userAssignmentsList.size() > 0) ) {
            clusterId = userAssignmentsList.get(0).cluster();
        }

        final int curClusterId = clusterId;
        JavaRDD<PowerIterationClustering.Assignment> res = mGroups.filter(
                new Function<PowerIterationClustering.Assignment, Boolean>() {
                    public Boolean call(PowerIterationClustering.Assignment assignment) throws Exception {
                        if ( (assignment.cluster() == curClusterId) && (assignment.id() != curUserId) ) {
                            return true;
                        }
                        else {
                            return false;
                        }
                    }
                });

        // debugging starts here
        List<PowerIterationClustering.Assignment> resList = res.collect();
        for (PowerIterationClustering.Assignment assign : resList) {
            int cluster = assign.cluster();
            long uId = assign.id();
            MaxLogger.debug(PIC_UserClusterModel.class,
                            "----------------[filterClusterByUserId], [userId:" + uId + ", clusterId:" + cluster + "]");
        }
        // debugging ends here

        return new PIC_ClusterResult(res);
    }
}
