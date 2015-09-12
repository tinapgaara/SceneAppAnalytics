package z_try;

import main.java.TestUserProfileModelBuilder;
import maxA.common.Constants_TestOnly;
import maxA.io.AppLogRecord;
import maxA.io.decoder.IDecoder;
import maxA.main.SpecificRecord2Process;
import maxA.main.StreamProcessor;
import maxA.util.MaxLogger;
import org.apache.avro.specific.SpecificRecord;

import java.io.Serializable;

/**
 * Created by max2 on 7/25/2015.
 */
public class StreamProcessingThread extends Thread implements Serializable {

    public final static int SLEEP_TIME_InMilliseconds = 1000;
    public final static int TRIGGER_VECTOR_UPDATER_Threshold_min = 4;
    public final static int TRIGGER_VECTOR_UPDATER_Threshold_max = 20;

    public final static int TRIGGER_MODEL_UPDATER_THRESHOLD_max = 1;

    private IDecoder mDecoder;
    private StreamProcessor mStreamProcessor;

    private Records2Process mRecords2Process;
    private SpecificRecord2Process mSpecificRecord2Process;
    private static boolean mShouldStopFlag, mFinishFlag;

    private static int mTestCase; // TODO: For different test case, can be deleted in future when AppLog format has been corrected.

    public StreamProcessingThread(IDecoder decoder, StreamProcessor streamProcessor) {

        mDecoder = decoder;
        mStreamProcessor = streamProcessor;

        mRecords2Process = new Records2Process();
        mSpecificRecord2Process = new SpecificRecord2Process();

        mShouldStopFlag = false;
        mFinishFlag = false;
    }

    public StreamProcessingThread(IDecoder decoder, StreamProcessor streamProcessor, int testcase) {

        mDecoder = decoder;
        mStreamProcessor = streamProcessor;

        mRecords2Process = new Records2Process();
        mSpecificRecord2Process = new SpecificRecord2Process();

        mShouldStopFlag = false;
        mFinishFlag = false;

        mTestCase = testcase;
    }

    public IDecoder getDecoder() {
        return mDecoder;
    }

    public void recvNextRecord(byte[] nextRecord) { mRecords2Process.appendNextRecord(nextRecord); }

    public void recvNextRecord(SpecificRecord nextRecord) { mSpecificRecord2Process.appendNextRecord(nextRecord); }

    public void run() {
        int numRecords2Process = 0;
        while ( ! shouldStop() ) {
            try {
                SpecificRecord nextRecord = null;

                nextRecord = mSpecificRecord2Process.fetchNextRecord();
                if (nextRecord == null) {
                    if (numRecords2Process > 0) {
                            mStreamProcessor.process();
                            numRecords2Process = 0;
                    }
                    else if (shouldStop()){
                        setFinishFlag();
                    }
                    else {
                        // For debug information
                        MaxLogger.debug(StreamProcessingThread.class, "---------------- [sleep for a while] ----------------");
                        //

                        Thread.sleep(SLEEP_TIME_InMilliseconds);
                    }
                }
                else {
                    if ( Constants_TestOnly.TEST_FLAG ) {
                        // For debug information
                        MaxLogger.debug(StreamProcessingThread.class,
                                "---------------- [decode " + mSpecificRecord2Process.getRecords().size()+ "th record] ----------------:");
                        //

                        AppLogRecord appLogRecord = TestUserProfileModelBuilder.generateAppLogRecord_TestOnly(numRecords2Process);
                        mDecoder.addAppLogRecord(appLogRecord);
                    }
                    else {
                        // For debug information
                        MaxLogger.debug(StreamProcessingThread.class,
                                "---------------- [decode " + mSpecificRecord2Process.getRecords().size()+ "th record] ----------------:");
                        //
                        mDecoder.decode(nextRecord);
                    }

                    numRecords2Process ++;
                    if (numRecords2Process > TRIGGER_VECTOR_UPDATER_Threshold_max ) {
                        mStreamProcessor.process();
                        numRecords2Process = 0;
                    }
                }
            }
            catch(InterruptedException ex) {
                // nothing to do here
            }
        }
        setFinishFlag();
    }

    public synchronized boolean shouldStop() {
        return (mShouldStopFlag && mSpecificRecord2Process.isEmpty());
    }

    public synchronized void setShouldStopFlag() {
        mShouldStopFlag = true;
    }

    public synchronized boolean isFinished() {
        return mFinishFlag;
    }

    public synchronized void setFinishFlag() {

        mFinishFlag = true;
    }
}