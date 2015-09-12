package maxA.io.filter;

import avro.AppLogs.AppLogs;

/**
 * Created by TAN on 7/5/2015.
 */
public interface IFilter {

    public boolean isNoise(AppLogs appLogs);
}
