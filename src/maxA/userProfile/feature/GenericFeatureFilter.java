package maxA.userProfile.feature;

import maxA.io.AppLogRecord;
import maxA.io.IRecord;
import maxA.io.sparkClient.SparkContext;
import maxA.userProfile.IFeatureFilter;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by TAN on 7/25/2015.
 */
public abstract class GenericFeatureFilter implements IFeatureFilter, Serializable {

    protected boolean mFilter4TrainDataFlag ;

    @Override
    public void setTrainDataFilterFlag(boolean filter4TrainData) {
        mFilter4TrainDataFlag = filter4TrainData;
    }

    @Override
    public boolean isTrainDataFilter() {
        return mFilter4TrainDataFlag;
    }

}
