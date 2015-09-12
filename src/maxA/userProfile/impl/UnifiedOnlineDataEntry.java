package maxA.userProfile.impl;

import maxA.userProfile.IOnlineDataEntry;

/**
 * Created by max2 on 8/1/2015.
 */
public class UnifiedOnlineDataEntry<T> implements IOnlineDataEntry {

    protected T mDataEntry;

    public UnifiedOnlineDataEntry(T dataEntry) {
        mDataEntry = dataEntry;
    }

    public T getDataEntry() {
        return mDataEntry;
    }
}
