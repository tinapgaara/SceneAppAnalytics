package maxA.io.sparkClient;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.deploy.SparkHadoopUtil;

import java.io.IOException;

/**
 * Created by TAN on 6/22/2015.
 */
public class CustomRecordReader extends RecordReader<LongWritable, BytesWritable> {

    private long mRecordKey;
    private BytesWritable mRecordValue;

    private boolean mNewFileArrived;

    public CustomRecordReader() {

        mRecordKey = 0;
        mRecordValue = null;
        mNewFileArrived = false;
    }

    /*
    * When a new file comes in, triggers initialize()
    * */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        mRecordValue = null;;

        // the file input
        int fileLength = (int) inputSplit.getLength();
        if (fileLength > 0) {
            mRecordKey++;
            mNewFileArrived = true;

            FileSplit fileSplit = (FileSplit) inputSplit;
            Path path = fileSplit.getPath();
            org.apache.hadoop.conf.Configuration conf = SparkHadoopUtil.get().
                    getConfigurationFromJobContext(taskAttemptContext);
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream fileInputStream = fs.open(path);
            byte[] buffer = new byte[fileLength];
            fileInputStream.readFully(buffer);
            mRecordValue = new BytesWritable(buffer); // get record value from buffer

            fileInputStream.close();
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
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {

        mNewFileArrived = false;
        return mRecordValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        return 0;
    }

    @Override
    public void close() throws IOException {

        mRecordValue = null;
    }
}
