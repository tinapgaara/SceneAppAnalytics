package z_try;

/**
 * Created by TAN on 7/01/2015.
 */
public class MultiRecords {

    private byte[] mContent;
    private int mLength;
    private int mCurStartPos;

    private int mCurSeparateCount;
    private char mSeparatorChar;

    public MultiRecords(byte[] content, char separatorChar) {
        mContent = content;
        mLength = (content == null) ? 0 : content.length;
        mCurStartPos = 0;

        mCurSeparateCount = 0;
        mSeparatorChar = separatorChar;
    }

    public boolean hasNextRecord() {
        return (mCurStartPos < mLength);
    }

    public byte[] nextRecord() {
        if (mCurStartPos >= mLength) {
            return null;
        }

        int endPos = -1;
        int pos = mCurStartPos;
        while (pos < mLength) {
            if (mContent[pos] == mSeparatorChar) {
                endPos = pos;
                break;
            }
            pos++;
        }
        if (endPos < 0) {
            endPos = mLength;
        }

        byte[] nextRecord = null;
        int nextRecordLength = endPos - mCurStartPos;
        if (nextRecordLength > 0) {
            nextRecord = new byte[nextRecordLength];
            for (int i = 0; i < nextRecordLength; i++) {
                nextRecord[i] = mContent[mCurStartPos + i];
            }
        }

        mCurStartPos = endPos + 1;

        return nextRecord;
    }

    public void release() {
        mContent = null;
        mLength = 0;
    }
}
