package maxA.userProfile;

import maxA.io.AppLogRecord;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

/**
 * Created by TAN on 7/5/2015.
 */
public interface IOnlineDataGenerator {

    public IOnlineData generateOnlineDataByAppLogs(
            IFeature feature, JavaRDD<AppLogRecord> data);


}
