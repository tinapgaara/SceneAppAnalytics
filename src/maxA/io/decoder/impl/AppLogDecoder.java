package maxA.io.decoder.impl;


import avro.AppLogs.AppLogSchemaHelper;
import avro.AppLogs.AppLogs;
import maxA.io.AppLogRecord;
import maxA.io.IRecord;
import maxA.userProfile.IFeatureFilter;
import maxA.io.decoder.IDecoder;
import maxA.io.filter.impl.AppLogFilter;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.avro.specific.SpecificRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by TAN on 7/5/2015.
 */
public class AppLogDecoder implements IDecoder, Serializable {

    // filters
    private AppLogFilter mFilter;
    private List<IFeatureFilter> mFeatureFilters;

    public AppLogDecoder(AppLogFilter filter) {

        mFilter = filter;
        mFeatureFilters = null;
    }

    public List<IFeatureFilter> getFilters() {
        return mFeatureFilters;
    }

    @Override
    public String registerFeatureFilter(IFeatureFilter filter) {

        if (filter == null) {
            return null;
        }
        String filterName = filter.getName();

        if (mFeatureFilters == null) {
            mFeatureFilters = new ArrayList<IFeatureFilter>();
        }
        else {
            for (IFeatureFilter existFilter : mFeatureFilters) {
                if (filterName.equals(existFilter.getName())) {
                    MaxLogger.error(AppLogDecoder.class,
                                    ErrMsg.msg(ErrMsg.ERR_MSG_DuplicatedRecordFilterName, filterName));
                    return null;    // registertion failed
                }
            }
        }

        mFeatureFilters.add(filter);

        return filterName;  // registertion successful
    }

    @Override
    public void unregisterFeatureFilter(String filterName) {

        if ( (filterName == null) || (mFeatureFilters == null) ) {
            return;
        }

        IFeatureFilter existFilter;
        for (int i = 0; i < mFeatureFilters.size(); i++) {
            existFilter = mFeatureFilters.get(i);
            if (filterName.equals(existFilter.getName())) {
                mFeatureFilters.remove(i);
                return;
            }
        }
    }

    @Override
    public void decode(SpecificRecord data) {

        AppLogs appLogs = AppLogSchemaHelper.convertRecord2Avro(data);

        // do filtering with mFilter
        if (mFilter != null) {
            if ( mFilter.isNoise(appLogs) ) {
                MaxLogger.info(AppLogDecoder.class,
                                ErrMsg.ERR_MSG_UnknownAppLogMessage);
                return;
            }
        }
        AppLogRecord appLogRecord = new AppLogRecord(appLogs);

        boolean isUseful = false;

        // do filtering with mRecordFilters
        if (mFeatureFilters != null) {
            for (IFeatureFilter recordFilter : mFeatureFilters) {
                if (recordFilter.isUsefulAppLog(appLogRecord)) {
                    isUseful = true;
                    recordFilter.appendAppLog(appLogRecord);
                }
            }
        }
        if ( ! isUseful) {
            // debugging starts here
            MaxLogger.debug(AppLogDecoder.class, "---------------- [not useful] ----------------");
            // debugging ends here
        }
    }

    /*
    @Override
    public IRecord[] getDecodedRecords() {

        if (mAppLogRecords == null) {
            return null;
        }

        AppLogRecord[] records = new AppLogRecord[mAppLogRecords.size()];
        mAppLogRecords.toArray(records);

        return records;
    }

    @Override
    public List<AppLogRecord> getDecodedAppLogsRecords() {

        if (mAppLogRecords == null) {
            return null;
        }

        return mAppLogRecords;
    }
    //*/

    @Override
    public void release() {

        mFilter = null;

        if (mFeatureFilters != null) {
            mFeatureFilters.clear();
            mFeatureFilters = null;
        }
        /*
        if (mAppLogRecords != null) {
            mAppLogRecords.clear();
            mAppLogRecords = null;
        }
        //*/
    }

    // For the fixed data which are generated inside of code
    // TODO: can be deleted when the AppLogs format has been corrected
    @Override
    public void addAppLogRecord(AppLogRecord appLogRecord) {

        boolean isUseful = true;

        // do filtering with mRecordFilters
        if (mFeatureFilters != null) {
            isUseful = false;

            for (IFeatureFilter recordFilter : mFeatureFilters) {
                if (recordFilter.isUsefulAppLog(appLogRecord)) {
                    isUseful = true;
                    recordFilter.appendAppLog(appLogRecord);
                }
            }
        }

        if ( ! isUseful) {
            // debugging starts here
            MaxLogger.debug(AppLogDecoder.class, "---------------- [not useful] ----------------");
            // debugging ends here
        }
    }
}
