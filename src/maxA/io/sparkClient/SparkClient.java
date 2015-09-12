package maxA.io.sparkClient;

import main.java.TestUserProfileModelBuilder;
import maxA.io.AppLogRecord;
import maxA.io.decoder.IDecoder;
import maxA.main.StreamProcessingThread;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import org.apache.hadoop.fs.Path;
import maxA.io.hdfsClient.HDFSClient;
import maxA.common.Constants_TestOnly;

/**
 * Created by TAN on 6/30/2015.
 */
public class SparkClient implements Serializable {

    private static SparkClient m_instance = null;

    private static final int SLEEP_TIME_SwitchToMonitorNextDir = 60000; // in milli-second

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
     * Read Avro file from HDFS
     * Decode it as a list of SpecificRecords
     * @return void
     */
    public void readFileFromHDFS(String filePath, IDecoder decoder) {

        MaxLogger.info(SparkClient.class, "---------------- [readAvroFileFromHDFS] BEGIN ...");

        try {
            Path path = new Path(filePath);
            Configuration config = HDFSClient.getConfig();
            SeekableInput input = new FsInput(path, config);
            DatumReader<SpecificRecord> reader = new SpecificDatumReader<SpecificRecord>();
            FileReader<SpecificRecord> fileReader = DataFileReader.openReader(input, reader);

            decodeMultiRecords(decoder, fileReader);

            fileReader.close();
        } catch (java.io.IOException e) {
            MaxLogger.error(SparkClient.class, ErrMsg.ERR_MSG_NotAvroFile);
        }
        MaxLogger.info(SparkClient.class, "---------------- [readAvroFileFromHDFS] END ");
    }

    /**
     * Read avro file from HDFS in streams
     * Push to the records' queue
     * Change listening directory automatically
     * @return void
     */
    public void processStreamFiles(final String testHdfsPath, StreamProcessingThread thread) {

        JavaPairInputDStream<LongWritable, String> inputDStream = null;

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
                    (( ! curMonitorDir.equals(dirByNow))) ) { // && isDirExist(testHdfsPath + dirByNow))) {

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
                        testHdfsPath + curMonitorDir, LongWritable.class, String.class, CustomInputFormatStr.class);

                final String curPath = testHdfsPath + curMonitorDir;
                JavaDStream<AppLogRecord> content = inputDStream.flatMap(new FlatMapFunction<Tuple2<LongWritable, String>, AppLogRecord>() {
                    @Override
                    public Iterable<AppLogRecord> call(Tuple2<LongWritable, String> tuple2) throws Exception {

                        MaxLogger.info(SparkClient.class, "----------------[processAvroStreamFile] ENTER ......" + testHdfsPath);

                        String curSubPath  = tuple2._2();

                        Configuration config = HDFSClient.getConfig();
                        Path path = new Path(curPath + curSubPath);
                        SeekableInput input = new FsInput(path, config);
                        DatumReader<SpecificRecord> reader = new SpecificDatumReader<SpecificRecord>();
                        try {
                            FileReader<SpecificRecord> fileReader = DataFileReader.openReader(input, reader);
                            for (SpecificRecord datum : fileReader) {
                                spThread.recvNextRecord(datum);
                            }

                        } catch (java.io.IOException e) {
                            MaxLogger.error(SparkClient.class, ErrMsg.ERR_MSG_NotAvroFile);
                        }

                        MaxLogger.info(SparkClient.class, "----------------[processAvroStreamFile] KEY = [" + tuple2._1().toString() + "]");
                        MaxLogger.info(SparkClient.class, "----------------[processAvroStreamFile] VAL-LENGTH = [" + tuple2._2().getBytes().length + "]");

                        return null; //spThread.getDecoder().getDecodedAppLogsRecords();
                    }
                });
                // TODO remove this statement, only used for debugging now
                content.print();
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
                MaxLogger.debug(SparkClient.class, "----------------[sleep:]" + lSleepTimeInMs);
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
            int cRecords = 0;
            while (fileReader.hasNext()) {
                SpecificRecord nextRecord = fileReader.next();
                cRecords++;
            }
            // test smaller data set
            AppLogRecord[] appLogRecords = TestUserProfileModelBuilder.generateAppLogRecords_TestOnly(cRecords);
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
            exist = HDFSClient.getInstance().ifExists(new Path(dir));
        }
        catch (Exception ex) {
            MaxLogger.error(SparkClient.class, ErrMsg.ERR_MSG_UnknownHDFSPath);
        }
        return exist;
    }

}