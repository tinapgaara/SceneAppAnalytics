package z_try;

import maxA.io.sparkClient.CustomRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by max2 on 8/6/15.
 */
public class CustomInputFormatByte extends FileInputFormat<LongWritable, BytesWritable> {

    public CustomInputFormatByte() {
        // nothing to do here
    }

    /*
    * Provide an RecordReader, to iterate through records in a given split,
    * and to parse each record into key and value of predefined type
    * */
    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        return new CustomRecordReader();
    }

    /**
     * Identify all files used as input data, and divide them into input splits.
     * Checks whether you can split a given file
     * True: if the file is larger than a block, will split it
     * False: treat each record as an atomic record
     */
    //Todo: change to true for large files in future
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

}
