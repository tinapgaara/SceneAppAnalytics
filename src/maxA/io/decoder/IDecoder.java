package maxA.io.decoder;

import maxA.io.AppLogRecord;
import maxA.io.IRecord;
import maxA.userProfile.IFeatureFilter;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

/**
 * Created by TAN on 7/5/2015.
 */
public interface IDecoder {

    public String registerFeatureFilter(IFeatureFilter filter);
    public void unregisterFeatureFilter(String filterName);

    public void decode(SpecificRecord data);

    public List<IFeatureFilter> getFilters();

    /*
    public IRecord[] getDecodedRecords();
    List<AppLogRecord> getDecodedAppLogsRecords();
    //*/

    public void release();

    public void addAppLogRecord(AppLogRecord appLogRecord);

}
