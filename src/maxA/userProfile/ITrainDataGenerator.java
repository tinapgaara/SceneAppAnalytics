package maxA.userProfile;

import maxA.io.AppLogRecord;
import maxA.io.IRecord;
import maxA.io.decoder.IDecoder;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by TAN on 7/5/2015.
 */
public interface ITrainDataGenerator {

    public ITrainData generateTrainDataByAppLogs(IFeature feature, JavaRDD<AppLogRecord> data);

    public ITrainData generateRandomTrainingData(IFeature feature);

}
