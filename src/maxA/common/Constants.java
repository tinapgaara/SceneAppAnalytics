package maxA.common;

import java.util.*;

/**
 * Created by TAN on 6/22/2015.
 */
public class Constants {

    public static final String APP_NAME = "max-analytics";
    public static final String SPARK_CONF_MASTER_local = "local";
    public static final String SPARK_CONF_MASTER_local2 = "local[2]";

    public static final String FS_DEFAULT_NAME = "fs.default.name";
    public static final String FS_DEFAULT_VALUE = "hdfs://10.0.14.58:8020";
    public static final String DFS_NAMENODE_NAME = "dfs.namenode.name.dir";
    public static final String DFS_NAMENODE_VALUE = "file:///var/lib/hadoop-hdfs/cache/hdfs/dfs/name";
    public static final String DFS_PERMISSION_GROUP = "dfs.permissions.superusergroup";
    public static final String DFS_PERMISSION_GROUP_VALUE = "hadoop";
    public static final String DFS_WEBHDFS_ENABLED = "dfs.webhdfs.enabled";
    public static final String DFS_WEBHDFS_ENABLED_VALUE = "true";

    public static final String HDFS_PATH = "hdfs://10.0.14.58/scene/kafka/channel";
    public static final String HDFS_PATH_DEV = "hdfs://10.0.14.58/scene/kafka/channel/dev";

    public static long STREAMING_DURATION_InMilliSeconds = 20000;

    public static final char LOG_RECORD_SEPARATOR_CHAR = '\n';

    public static final int THRESHOLD_EnterMerchantTimeLength_InSeconds = 120;
    public static final int THRESHOLD_EnterDetailsTimeLength_InSeconds = 120;

    public static int TRAIN_USER_PROFILE_MODEL_NumIterations = 20;
    public static int ALS_RECOMMENDER_MODEL_NumIterations = 10;
    public static double ALS_RECOMMENDER_MODEL_LAMBDA = 0.01;
    public static int ALS_RECOMMENDER_MODEL_RANK = 20;

    public static String TEST_STREAM_PATH = "/15-06-22/";

    public static String TEST_TRAIN_FILE_PATH = "/15-08-12/AppLogs-events.1439394860662";

    public static String TEST_Ratingood_FILE_PATH = "/15-08-06/AppLogs-events.1438878899887";
    public static String TEST_Ratingbad_FILE_PATH = "/15-08-06/AppLogs-events.1438879114654";
    public static String TEST_Atmosphere_FILE_PATH = "/15-08-06/AppLogs-events.1438879553787";
    public static String TEST_Queue_FILE_PATH = "/15-08-06/AppLogs-events.1438881019638";
    public static String TEST_EnterMerchant_FILE_PATH = "/15-08-06/AppLogs-events.1438881188762";
    public static String TEST_EnterMerchantDetails_FILE_PATH = "/15-08-06/AppLogs-events.1438881188762";

    public static String TEST_REPLY_FILE_PATH = "/15-08-06/AppLogs-events.1438881188762";
    public static String TEST_ASK_FILE_PATH = "/15-08-06/AppLogs-events.1438889704536";

    public static String TEST_INTEREST_FILE_PATH = "/15-08-06/AppLogs-events.1438891241241";
    public static String TEST_BOOKMARK_FILE_PATH = "/15-08-06/AppLogs-events.1438895296307";

    public static String REDIS_SERVER = "10.0.14.25"; // 10.0.14.28
    public static String REDIS_PORT = "6379";

    public static int REDIS_INTERTEST_NUMBER = 14;

    public static int LinearRegressionModelNo = 1;
    public static int LogisticRegressionModelNo = 2;
    public static int NaiveBayesModelNo = 3;
    public static int SVMModelNo = 4;

    public static int TESTModelNum = 4;

    public static double TrainDataGenerationThreshold = 0.5;
    public static int GeneratedTrainDataNum = 400000;


    public static final Map<Long, List<Long>> interest2Mapping;

    static
    {
        interest2Mapping = new HashMap<Long,List<Long>>();
        interest2Mapping.put(new Long(1), Arrays.asList(3404L,7462L,9015L,3408L,20606L,20627L,14425L,30511L));
        interest2Mapping.put(new Long(2), Arrays.asList(7888L,7365L,4428L,3257L,22902L,2936L,7862L));
        interest2Mapping.put(new Long(3), Arrays.asList(26027L));
        interest2Mapping.put(new Long(4), Arrays.asList(3291L));
        interest2Mapping.put(new Long(5), Arrays.asList(35976L));
        interest2Mapping.put(new Long(6), Arrays.asList(5043L,12986L));
//        interest2Mapping.put(7,Arrays.asList());
        interest2Mapping.put(new Long(8), Arrays.asList(3278L));
//        interest2Mapping.put(9,Arrays.asList());


    };
    /**************************************************************************
    public static String TEST_APP_MERCHANT_DETAIL_FILE_PATH = "/15-07-02/avro.AppLogs-events.1435862474824";
    public static String TEST_APP_SEARCH_FILE_PATH="/15-07-02/avro.AppLogs-events.1435863021218";
    public static String TEST_APP_FILTER_FILE_PATH="/15-07-02/avro.AppLogs-events.1435863598496";
    public static String TEST_APP_GIVE_FILE_PATH = "/15-07-02/avro.AppLogs-events.1435864022353";
    public static String TEST_APP_ENDOR_FILE_PATH = "/15-07-02/avro.AppLogs-events.1435864600578";
    public static String TEST_APP_ANSWER_FILE_PATH = "/15-07-02/avro.AppLogs-events.1435865121734";
    public static String TEST_APP_FAVOR_FILE_PATH = "/15-07-02/avro.AppLogs-events.1435865653086";
    public static String TEST_APP_INTEREST_FILE_PATH = "/test/15-07-02/avro.AppLogs-events.1435869627690";
    public static String TEST_APP_PLAN_LIKE_FILE_PATH = "/test/15-07-02/avro.AppLogs-events.1435872401330";

    public static String TEST_MIS_INTEREST_FILE_PATH = "/test/15-07-02/MisfitLogs-events.1435870497201";
    public static String TEST_MIS_PLAN_FILE_PATH = "/test/15-07-02/MisfitLogs-events.1435872401860";
    public static String TEST_MIS_ASK_FILE_PATH= "/test/15-07-02/MisfitLogs-events.1435875631577";

    public static String TEST_FILE_NAME = "Give-events.1434410817236";
    public static String TEST_WRITE_DIR_NAME = "testWriteDir";

     public static String TEST_STREAM_PATH = "/15-06-22/";

    public static final String BROKER_CONNECTION = "10.0.14.60:9092,10.0.14.61:9092";
    public static final String KAFKA_ZK__CONNECTION = "10.0.14.59:2181";
    public static final String GROUP_PREFIX = "ying";
    public static final String KAFKA_PARAM_BROKER_LIST = "metadata.broker.list";
    public static final String KAFKA_PARAM_ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String KAFKA_PARAM_GROUP_ID = "group.id";

    public static final String TOPICS = "avro.AppLogs,MisfitLogs,Give";
    public static final String GIVE_KAFKA_TOPIC = "Give";
    public static final String APP_LOGS_KAFKA_TOPIC = "avro.AppLogs";
    public static final String MISFIT_LOGS_KAFKA_TOPIC = "MisfitLogs";

    public static final String REDIS_ACTOR_ADDRESS = "ACTOR:ADDRESS:";
    public static final Integer REDIS_ACTOR_ADDRESS_TTL = -1;

    public static Properties getKafkaProducerProperties() {
        Properties props = new Properties();

        props.put("metadata.broker.list", BROKER_CONNECTION);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("request.required.acks", "1");
        props.put("bootstrap.servers", BROKER_CONNECTION);

        return props;
    }
    **************************************************************************/

    /**************************************************************************
    public static String LOG_PATTERN = "\\{\"action\":(\\S+)\\,\"value\":(\\S+)\\,\"id\":(\\S+)\\,\"contextName\":(\\S+)\\,\"contextValue\":(\\S+)\\}";
    public static String LOG_CONTEXT_SEPARATOR_CHAR = ",";
    public static String LOG_CONTEXT_ARR_PATTERN = "\\[(\\S+)\\]";
    public static String LOG_OR_SEPARATOR_CHAR = "||";
    public static String TEST_PATTERN_STR = "{\"action\":\"enter\",\"value\":null,\"id\":45,\"contextName\":\"[\"curLat\",\"curLng\"],\"contextValue\":\"[12.3,23.3]\"}";
    **************************************************************************/
}