package maxA.userProfile.impl;

import maxA.userProfile.IOnlineData;
import maxA.userProfile.IOnlineDataEntry;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 * Created by max2 on 8/1/2015.
 */
public class UnifiedOnlineData implements IOnlineData {

    private JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> mOnlinedata;

    public UnifiedOnlineData(JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> data) {

        // debugging starts here
        MaxLogger.debug(UnifiedOnlineData.class,
                        "----------------[Set Online Data]----------------");
        // debugging ends here

        mOnlinedata = data;
    }

    public JavaRDD<Tuple2<Tuple2<Long, Long>, IOnlineDataEntry>> getData() {

        return mOnlinedata;
    }

    @Override
    public void release() {
        mOnlinedata = null;
    }

}
