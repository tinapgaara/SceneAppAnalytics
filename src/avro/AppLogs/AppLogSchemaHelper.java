package avro.AppLogs;

import maxA.util.MaxLogger;
import org.apache.avro.specific.SpecificRecord;

/**
 * Created by max2 on 7/16/15.
 */
public class AppLogSchemaHelper {

    public static AppLogs convertRecord2Avro(SpecificRecord record) {

        AppLogs appLog = (AppLogs) record;

        // For debug information
        MaxLogger.debug(AppLogSchemaHelper.class, "---------------- [convertRecord2Avro] ------------------");
        System.out.println(appLog.toString());
        //

        return appLog;
    }
}
