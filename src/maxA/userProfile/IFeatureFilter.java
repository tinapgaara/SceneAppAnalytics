package maxA.userProfile;

import maxA.io.AppLogRecord;
import maxA.io.IRecord;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Created by TAN on 7/6/2015.
 */
public interface IFeatureFilter {

    public String getName();

    public void setTrainDataFilterFlag(boolean filter4TrainData);
    public boolean isTrainDataFilter();

    public boolean isUsefulAppLog(AppLogRecord record);

    public JavaRDD<AppLogRecord> getInputAppLogs();
    public boolean appendAppLog(AppLogRecord record);

    public void release();

}
