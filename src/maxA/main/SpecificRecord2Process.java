package maxA.main;

import maxA.util.MaxLogger;
import org.apache.avro.specific.SpecificRecord;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by max2 on 8/6/15.
 */
public class SpecificRecord2Process implements Serializable {

    private static Queue<SpecificRecord> mRecords;

    public SpecificRecord2Process() {
        mRecords = new LinkedList<SpecificRecord>();
    }

    public synchronized boolean isEmpty() {
        return mRecords.isEmpty();
    }

    public Queue<SpecificRecord> getRecords() {
        return mRecords;
    }

    public synchronized void appendNextRecord(SpecificRecord record) {

        mRecords.add(record);
        // For debug information
        MaxLogger.debug(SpecificRecord2Process.class,
                        "---------------- [appendNextRecord] Record's size:" + mRecords.size() + " ----------------");
        //
    }

    public synchronized SpecificRecord fetchNextRecord() {

        if (mRecords.isEmpty()) {
            // For debug information
            MaxLogger.debug(SpecificRecord2Process.class,
                            "---------------- [fetchNextRecord] : NULL mecords ----------------");
            //
            return null;
        }
        else {
            return mRecords.remove();
        }
    }
}
