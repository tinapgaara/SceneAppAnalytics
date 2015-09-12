package maxA.io.sparkClient;

import maxA.common.Constants;
import maxA.util.MaxLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by TAN on 6/23/2015.
 */
public class SparkContext {

    private static JavaSparkContext m_jsc = null;
    private static JavaStreamingContext m_jssc = null;

    private static boolean jscStartFlag = false;
    private static boolean jsscStartFlag = false;

    public static JavaSparkContext getSparkContext() {

        if (m_jsc == null) {
            if (jscStartFlag) {
                SparkConf conf = new SparkConf()
                        .setAppName(Constants.APP_NAME)
                        .setMaster(Constants.SPARK_CONF_MASTER_local);

                m_jsc = new JavaSparkContext(conf);
            }
        }
        return m_jsc;
    }

    public static JavaStreamingContext getJavaStreamingContext() {

        if (m_jssc == null) {
            if (jsscStartFlag) {
                SparkConf conf = new SparkConf()
                        .setAppName(Constants.APP_NAME)
                        .setMaster(Constants.SPARK_CONF_MASTER_local2);

                m_jssc = new JavaStreamingContext(conf,
                        new Duration(Constants.STREAMING_DURATION_InMilliSeconds));
            }
        }

        return m_jssc;
    }

    public static void setJscStartFlag() {

        if ( jsscStartFlag ) {
            m_jssc.stop();
            jsscStartFlag = false;
            m_jssc = null;
        }
        jscStartFlag = true;
    }

    public static void setJsscStartFlag() {

        if ( jscStartFlag ) {
            m_jsc.stop();
            jscStartFlag = false;
            m_jsc = null;
        }
        jsscStartFlag = true;
    }

    public static boolean getJscStartFlag() {
        return jscStartFlag;
    }

    public static boolean getJsscStartFlag() {
        return jsscStartFlag;
    }

    public static JavaStreamingContext startNewJavaStreamingContext() {

        if (m_jssc != null) {
            m_jssc = null;
        }

        m_jssc = getJavaStreamingContext();
        MaxLogger.info(SparkContext.class, "----------------[StartNewJavaStreamingContext]----------------");

        return m_jssc;
    }
}
