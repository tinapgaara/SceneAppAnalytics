package z_try;

import maxA.util.MaxLogger;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by max2 on 7/26/2015.
 */
public class Records2Process implements Serializable {

    private static Queue<byte[]> mRecords;

    public Records2Process() {
        mRecords = new LinkedList<byte[]>();
    }

    public synchronized boolean isEmpty() {
        return mRecords.isEmpty();
    }

    public Queue<byte[]> getRecords() {
        return mRecords;
    }

    public synchronized void appendNextRecord(byte[] record) {

        mRecords.add(record);
        // For debug information
        MaxLogger.debug(Records2Process.class,
                        "---------------- [appendNextRecord] Record's size:" +mRecords.size()+ " ----------------" );
        //
    }

    public synchronized byte[] fetchNextRecord() {

        if (mRecords.isEmpty()) {
            // For debug information
            MaxLogger.debug(Records2Process.class,
                            "---------------- [fetchNextRecord] : NULL Records ----------------");
            //
            return null;
        }
        else {
            return mRecords.remove();
        }
    }
}