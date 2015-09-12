package maxA.main;

import maxA.io.sparkClient.SparkClient;

import java.io.Serializable;

/**
 * Created by TAN on 8/6/2015.
 */
public class SparkStreamThread extends Thread implements Serializable {

    private SparkClient mSparkClient;

    private String mHdfsPath;

    private StreamProcessingThread mStreamProcessingThread;

    private int mTestCase; // For testing, can be deleted when AppLogs' format are corrected

    public SparkStreamThread(SparkClient sparkClient, String hdfsPath,
                             StreamProcessingThread streamProcessingThread) {

        mSparkClient = sparkClient;
        mHdfsPath = hdfsPath;
        mStreamProcessingThread = streamProcessingThread;
    }

    public SparkStreamThread(SparkClient sparkClient, String hdfsPath,
                             StreamProcessingThread streamProcessingThread, int testcase) {

        mSparkClient = sparkClient;
        mHdfsPath = hdfsPath;
        mStreamProcessingThread = streamProcessingThread;

        mTestCase = testcase;
    }

    public void run() {

        mSparkClient.processStreamFiles(mHdfsPath, mStreamProcessingThread);
        mStreamProcessingThread.setShouldStopFlag();
    }
}
