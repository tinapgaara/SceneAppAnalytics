package maxA.cluster;

import org.apache.spark.api.java.JavaRDD;

/**
 * Created by max2 on 8/1/2015.
 */
public interface IClusterData<T> extends java.io.Serializable {

    public JavaRDD<T> getData();
    public void setData(JavaRDD<T> clusterData);

    public void release() ;
}
