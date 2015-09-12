package z_try.sparkClient;

import maxA.common.Constants_TestOnly;
import maxA.io.AppLogRecord;
import maxA.io.decoder.IDecoder;
import z_try.CustomInputFormatByte;
import maxA.io.sparkClient.CustomInputFormatStr;
import maxA.io.sparkClient.SparkContext;
import z_try.StreamProcessingThread;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import z_try.MultiRecords;
import maxA.common.Constants;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Created by max2 on 6/30/15.
 */
public class SparkClient {

    private static String TEST_APP_ASK_FILE_PATH = "/15-07-02/avro.AppLogs-events.1435865121734";

    private static final int SLEEP_TIME_SwitchToMonitorNextDir = 60000; // in milli-second

    private static SparkClient m_instance = null;

    private SparkClient() {
        // nothing to do here
    }

    public static SparkClient getInstance() {
        if (m_instance == null) {
            m_instance = new SparkClient();
        }
        return m_instance;
    }

    /**
     * Read binary file from HDFS using API: binaryFile
     * Decode it as a list of byte array
     * @return void
     */
    public void readBinaryFileFromHDFS(String filePath, IDecoder decoder) {
        MaxLogger.info(SparkClient.class, "---------------- [readBinaryFileFromHDFS] BEGIN ...");

        SparkContext.setJscStartFlag();
        JavaSparkContext sc = SparkContext.getSparkContext();
        JavaPairRDD<String, PortableDataStream> rdd = sc.binaryFiles(filePath);

        final IDecoder curDecoder = decoder;

        JavaPairRDD<String, AppLogRecord> res = rdd.flatMapValues(
                new Function<PortableDataStream, Iterable<AppLogRecord>>() {
                    public Iterable<AppLogRecord> call(PortableDataStream data) throws Exception {

                        byte[] content = data.toArray();

                        MultiRecords multiRecords = new MultiRecords(content, Constants.LOG_RECORD_SEPARATOR_CHAR);

                        decodeMultiRecords(curDecoder, multiRecords);

                        multiRecords.release();
                        return null;
                    }
                });
        if (res == null) {
            MaxLogger.info(SparkClient.class, "----------------[readBinaryFileFromHDFS] RES = NULL");
        }
        else {
            MaxLogger.info(SparkClient.class, "----------------[readBinaryFileFromHDFS] RES-length = [" + res.count() + "]");
        }

        MaxLogger.info(SparkClient.class, "---------------- [readBinaryFileFromHDFS] END ");

    }


    /**
     * Read binary file from HDFS in streams
     * Push to the records' queue
     * Change listening directory automatically
     * @return void
     */
    public void processStreamBinaryFiles(final String testHdfsPath, StreamProcessingThread thread) {

        JavaPairInputDStream<LongWritable, BytesWritable> inputDStream = null;

        final StreamProcessingThread spThread = thread;

        LocalDateTime now, nextDay;
        String curMonitorDir = null, dirByNow;
        JavaStreamingContext jssc = null;

        while (true) {
            now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("/yy-MM-dd/");
            dirByNow = now.format(formatter);
            // For debug information
            MaxLogger.info(SparkClient.class, "----------------[dirByNow]:"+dirByNow + "----------------");
            //

            if ((curMonitorDir == null) ||
                    ((!curMonitorDir.equals(dirByNow))&& isDirExist(testHdfsPath + dirByNow))) {

                curMonitorDir = dirByNow;

                if (jssc != null) {
                    jssc.stop(false); // stopSparkContext = false
                    jssc.awaitTermination();
                    jssc.close();
                    jssc = null;
                }
            }
            if (jssc == null) {
                SparkContext.setJsscStartFlag();
                jssc = SparkContext.startNewJavaStreamingContext();

                inputDStream = jssc.fileStream(
                        testHdfsPath + curMonitorDir , LongWritable.class, BytesWritable.class, CustomInputFormatByte.class);

                JavaDStream<AppLogRecord> content = inputDStream.flatMap(new FlatMapFunction<Tuple2<LongWritable, BytesWritable>, AppLogRecord>() {
                    @Override
                    public Iterable<AppLogRecord> call(Tuple2<LongWritable, BytesWritable> tuple2) throws Exception {

                        MaxLogger.info(SparkClient.class,
                                "----------------[processStreamBinaryFilesDynamically] ENTER ......" + testHdfsPath);

                        byte[] content = tuple2._2().getBytes();

                        MaxLogger.info(SparkClient.class,
                                "----------------[processStreamBinaryFilesDynamically] KEY = [" + tuple2._1().toString() + "]");
                        MaxLogger.info(SparkClient.class,
                                "----------------[processStreamBinaryFilesDynamically] VAL-LENGTH = [" + tuple2._2().getBytes().length + "]");

                        MultiRecords multiRecords = new MultiRecords(content, Constants.LOG_RECORD_SEPARATOR_CHAR);
                        while (multiRecords.hasNextRecord()) {
                            byte[] nextRecord = multiRecords.nextRecord();
                            spThread.recvNextRecord(nextRecord);
                        }
                        multiRecords.release();
                        return null;
                    }
                });

                if (content == null) {
                    MaxLogger.info(SparkClient.class, "----------------[processStreamBinaryFilesDynamically] CONTENT = NULL");
                }
                else {
                    content.print();
                }
                jssc.start();
            }

            long lSleepTimeInMs = SLEEP_TIME_SwitchToMonitorNextDir;
            if (curMonitorDir.equals(dirByNow)) {
                nextDay = now.plusDays(1);
                LocalDateTime nextDayBeginning = LocalDateTime.of(
                        nextDay.getYear(),
                        nextDay.getMonth(),
                        nextDay.getDayOfMonth(),
                        0, 0, 0);    // hour = 0, minute = 0, second = 0
                long seconds = now.until(nextDayBeginning, ChronoUnit.SECONDS);
                lSleepTimeInMs = seconds * 1000;
            }
            try {
                // For debug information
                MaxLogger.debug(SparkClient.class, "----------------[sleep:]" + lSleepTimeInMs + " ms");
                //
                Thread.sleep(lSleepTimeInMs);
            }
            catch (InterruptedException ex) {
                // nothing to do here
            }
        }
    }

    /**
     * Read binary file from HDFS in streams
     * Push to the records' queue
     * @return void
     */
    public void processStreamFiles(String testHdfsPath, StreamProcessingThread thread) { // TODO: can be deleted

        SparkContext.setJsscStartFlag();
        JavaStreamingContext jssc = SparkContext.getJavaStreamingContext();
        JavaPairInputDStream<LongWritable, String> inputDStream = jssc.fileStream(
                testHdfsPath, LongWritable.class, String.class, CustomInputFormatStr.class);

        final StreamProcessingThread spThread = thread;
        final String curPath = testHdfsPath;
        JavaDStream<AppLogRecord> content = inputDStream.flatMap(new FlatMapFunction<Tuple2<LongWritable, String>, AppLogRecord>() {
            @Override
            public Iterable<AppLogRecord> call(Tuple2<LongWritable, String> tuple2) throws Exception {

                MaxLogger.info(SparkClient.class, "----------------[processStreamFiles] ENTER ......");

                String curSubPath  = tuple2._2();

                Configuration config = maxA.io.hdfsClient.HDFSClient.getConfig();
                Path path = new Path(curPath + curSubPath);
                SeekableInput input = new FsInput(path, config);
                DatumReader<SpecificRecord> reader = new SpecificDatumReader<SpecificRecord>();
                FileReader<SpecificRecord> fileReader = DataFileReader.openReader(input, reader);

                for (SpecificRecord datum : fileReader) {
                    spThread.recvNextRecord(datum);
                }

                MaxLogger.info(SparkClient.class, "----------------[processStreamFiles] KEY = [" + tuple2._1().toString() + "]");
                MaxLogger.info(SparkClient.class, "----------------[processStreamFiles] VAL PATH = [" + curPath + curSubPath + "]");

                return null;// spThread.getDecoder().getDecodedAppLogsRecords();
            }
        });
        if (content == null) {
            MaxLogger.info(SparkClient.class, "----------------[processStreamFiles] CONTENT = NULL");
        }
        else {
            content.print();
        }
        jssc.start();
        jssc.awaitTermination();
        MaxLogger.info(SparkClient.class, "----------------[processStreamFiles] END.");
    }

    /**
     * Test:
     * Read binary file from HDFS in streams
     * Then, push to the records' queue
     * Then, can change listening directory automatically
     * @return void
     */

    public void testProcessStreamFilesDynamically(final String testHdfsPath, StreamProcessingThread thread) {

        JavaPairInputDStream<LongWritable, String> inputDStream = null;

        final StreamProcessingThread spThread = thread;

        LocalDateTime now, nextDay;
        String curMonitorDir = null, dirByNow;
        JavaStreamingContext jssc = null;

        while (true) {
            System.out.println("----------------[0000000000]");
            now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("/yy-MM-dd/");
            dirByNow = now.format(formatter);
            if (curMonitorDir != null)
                dirByNow = "/15-07-27/";
            else
                dirByNow = "/15-07-28/";
            System.out.println("----------------[dirByNow:]" + dirByNow);

            if ((curMonitorDir == null) ||
                    ((!curMonitorDir.equals(dirByNow)))){ //&& isDirExist(config, testHdfsPath + dirByNow))) {

                curMonitorDir = dirByNow;
                System.out.println("----------------[11111111]" + curMonitorDir);

                if (jssc != null) {
                    jssc.stop(false); // stopSparkContext = false
                    jssc.awaitTermination();
                    jssc.close();
                    jssc = null;
                }
            }
            System.out.println("----------------" + "[22222222]" + curMonitorDir);// always be true

            if (jssc == null) {
                //jssc = new JavaStreamingContext(sparkConf, new Duration(durationInMillis));
                SparkContext.setJsscStartFlag();
                jssc = SparkContext.startNewJavaStreamingContext();

                inputDStream = jssc.fileStream(
                        testHdfsPath + curMonitorDir , LongWritable.class, String.class, CustomInputFormatStr.class);

                final String curPath = testHdfsPath + curMonitorDir;
                JavaDStream<AppLogRecord> content = inputDStream.flatMap(new FlatMapFunction<Tuple2<LongWritable, String>, AppLogRecord>() {
                    @Override
                    public Iterable<AppLogRecord> call(Tuple2<LongWritable, String> tuple2) throws Exception {

                        MaxLogger.info(SparkClient.class, "----------------[processStreamFilesDynamically] ENTER ......" + testHdfsPath);

                        String curSubPath  = tuple2._2();

                        Configuration config = maxA.io.hdfsClient.HDFSClient.getConfig();
                        Path path = new Path(curPath + curSubPath);
                        SeekableInput input = new FsInput(path, config);
                        DatumReader<SpecificRecord> reader = new SpecificDatumReader<SpecificRecord>();
                        FileReader<SpecificRecord> fileReader = DataFileReader.openReader(input, reader);

                        for (SpecificRecord datum : fileReader) {
                            spThread.recvNextRecord(datum);
                        }

                        MaxLogger.info(SparkClient.class, "----------------[processStreamFilesDynamically] KEY = [" + tuple2._1().toString() + "]");
                        MaxLogger.info(SparkClient.class, "----------------[processStreamFilesDynamically] VAL-LENGTH = [" + tuple2._2().getBytes().length + "]");

                        return null; //spThread.getDecoder().getDecodedAppLogsRecords();
                    }
                });

                if (content == null) {
                    MaxLogger.info(SparkClient.class, "----------------[processStreamFilesDynamically] CONTENT = NULL");
                }
                else {
                    content.print();
                }

                jssc.start();
                System.out.println("----------------[processStreamFilesDynamically] this round END.");
            }
            else {
                System.out.println("----------------[444444444]");
            }

            long lSleepTimeInMs = SLEEP_TIME_SwitchToMonitorNextDir;
            if (curMonitorDir.equals(dirByNow)) {
                nextDay = now.plusDays(1);
                LocalDateTime nextDayBeginning = LocalDateTime.of(
                        nextDay.getYear(),
                        nextDay.getMonth(),
                        nextDay.getDayOfMonth(),
                        0, 0, 0);    // hour = 0, minute = 0, second = 0
                long seconds = now.until(nextDayBeginning, ChronoUnit.SECONDS);
                System.out.println("----------------[wait for next day:]" + seconds);
                lSleepTimeInMs = seconds * 1000;
            }
            try {
                lSleepTimeInMs = lSleepTimeInMs / 3600;
                System.out.println("----------------[sleep:]" + lSleepTimeInMs);
                Thread.sleep(lSleepTimeInMs);
            }
            catch (InterruptedException ex) {
                // nothing to do here
            }
        }
    }

    /**
     * Decode each AppLog as a SpecificRecord
     * 1) For testing (Constants_TestOnly.TEST_FLAG == true), generate different fixed types of AppLogRecords
     * 2) For real data (Constants_TestOnly.TEST_FLAG == false), generate real AppLogRecord
     * @return void
     */
    private void decodeMultiRecords(IDecoder decoder, FileReader<SpecificRecord> fileReader) {

        // TODO: remove test code here in future
        if (Constants_TestOnly.TEST_FLAG) {
            int recordNum = 0;
            while (fileReader.hasNext()) {
                SpecificRecord record = fileReader.next();
                recordNum ++;
            }
            // test smaller data set
            AppLogRecord[] appLogRecords = main.java.TestUserProfileModelBuilder.generateAppLogRecords_TestOnly(recordNum);
            if (appLogRecords != null) {
                for (AppLogRecord record : appLogRecords) {
                    decoder.addAppLogRecord(record);
                }
            }
        }
        else {
            for (SpecificRecord datum : fileReader) {
                decoder.decode(datum);
            }
        }
    }

    private static boolean isDirExist(String dir) {

        boolean exist = false;

        try {
            exist = maxA.io.hdfsClient.HDFSClient.getInstance().ifExists(new Path(dir));
        }
        catch (Exception ex) {
            MaxLogger.error(SparkClient.class, ErrMsg.ERR_MSG_UnknownHDFSPath);
        }

        return exist;
    }

    /**
     * Decode each AppLog as a byte array
     * 1) For testing (Constants_TestOnly.TEST_FLAG == true), generate different fixed types of AppLogRecords
     * 2) For real data (Constants_TestOnly.TEST_FLAG == false), generate real AppLogRecord
     * @return void
     */
    private void decodeMultiRecords(IDecoder decoder, MultiRecords multiRecords) {

        // TODO: remove test code here in future
        if (Constants_TestOnly.TEST_FLAG) {
            int recordNum = 0;
            while (multiRecords.hasNextRecord()) {
                byte[] nextRecord = multiRecords.nextRecord();
                recordNum ++ ;
            }

            // test smaller data set
            AppLogRecord[] appLogRecords = main.java.TestUserProfileModelBuilder.generateAppLogRecords_TestOnly(recordNum);
            if (appLogRecords != null) {
                for (AppLogRecord record : appLogRecords) {
                    decoder.addAppLogRecord(record);
                }
            }
        }
        else {
            while (multiRecords.hasNextRecord()) {
                byte[] nextRecord = multiRecords.nextRecord();
                // decoder.decode(nextRecord);
            }
        }
    }

    private void waitForStreamProcessingThread(StreamProcessingThread spThread) {

        spThread.setShouldStopFlag();

        while (!spThread.isFinished()) {

            try {
                MaxLogger.info(SparkClient.class, "---------------- [wait for processing thread] ---------------- ");
                Thread.sleep((StreamProcessingThread.SLEEP_TIME_InMilliseconds));
            } catch (InterruptedException ex) {
                // nothing to do here
            }
        }
    }


}