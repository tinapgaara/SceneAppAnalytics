package maxA.recommender;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

/**
 * Created by max2 on 8/4/15.
 */
public interface IRecommenderData<T> extends java.io.Serializable {

    public JavaRDD<T> getData();
    public void setData(JavaRDD<T> recommenderData);

    public void release();
}
