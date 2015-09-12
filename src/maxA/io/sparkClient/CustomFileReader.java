package maxA.io.sparkClient;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by max2 on 8/5/15.
 */
public class CustomFileReader extends RecordReader<LongWritable, String> {

    private long mRecordKey;
    private boolean mNewFileArrived;
    private String mFileName;

    public CustomFileReader() {
        mRecordKey = 0;
        mFileName = null;
        mNewFileArrived = false;
    }

    /*
    * When a new file comes in, triggers initialize()
    * */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        mFileName = null;
        // the file input
        int fileLength = (int) inputSplit.getLength();
        if (fileLength > 0) {
            mRecordKey++;
            mNewFileArrived = true;

            FileSplit fileSplit = (FileSplit) inputSplit;
            Path path = fileSplit.getPath();
            mFileName =  path.getName();

        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return mNewFileArrived;
    }

    /*
    * If nextKeyValue() returns true, triggers getCurrentKey(), getCurrentValue()
    * */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(mRecordKey);
    }

    @Override
    public String getCurrentValue() throws IOException, InterruptedException {
        mNewFileArrived = false;
        return mFileName;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        mFileName = null;
    }

}
