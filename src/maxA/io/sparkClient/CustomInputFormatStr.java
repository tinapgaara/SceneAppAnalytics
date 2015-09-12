package maxA.io.sparkClient;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by TAN on 6/22/2015.
 * FileInputFormat: take care of file splitting
 */
public class CustomInputFormatStr extends FileInputFormat<LongWritable, String> {

    public CustomInputFormatStr() {
        // nothing to do here
    }

    /*
    * Provide an RecordReader, to iterate through records in a given split,
    * and to parse each record into key and value of predefined type
    * */
    @Override
    public RecordReader<LongWritable, String> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        return new CustomFileReader();
    }

    /**
     * Identify all files used as input data, and divide them into input splits.
     * Checks whether you can split a given file
     * True: if the file is larger than a block, will split it
     * False: treat each record as an atomic record
     */

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

}
