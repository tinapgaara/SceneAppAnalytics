package maxA.cluster;

import org.apache.spark.api.java.JavaRDD;

/**
 * Created by max2 on 2015/8/1.
 */
public interface IClusterResult<T> extends java.io.Serializable {

    public JavaRDD<T> getResult();
    public void setResult(JavaRDD<T> result);

    public void release();

}
