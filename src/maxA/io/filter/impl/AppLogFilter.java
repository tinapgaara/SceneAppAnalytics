package maxA.io.filter.impl;

import avro.AppLogs.ActionsType;
import avro.AppLogs.AppLogs;
import avro.AppLogs.HeadersType;
import avro.AppLogs.LogItemType;
import maxA.io.filter.IFilter;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;

import java.io.Serializable;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by TAN on 7/5/2015.
 */
public class AppLogFilter implements IFilter, Serializable {

    public static String FILTER_ACTION_PATTERN =
            "(reply)|(give)|(bookmark)|(unbookmark)|(ask)|(search)|(filter)|(enter)|(vote)|(endorse)|(interest)|(add)|(leave)|(join)";


    public AppLogFilter() {
        // nothing to do here
    }

    @Override
    public boolean isNoise(AppLogs appLogs) {

        LogItemType itemType = appLogs.getLogItem();
        if (itemType == null) {
            MaxLogger.error(AppLogFilter.class, ErrMsg.ERR_MSG_NullLogItemType);
            return true;
        }

        HeadersType headersType = itemType.getHeaders();
        if (headersType == null) {
            MaxLogger.error(AppLogFilter.class, ErrMsg.ERR_MSG_NullHeadersType);
            return true;
        }

        Long userId = headersType.getUserId();
        if (userId == null) {
            MaxLogger.error(AppLogFilter.class, ErrMsg.ERR_MSG_NullUserId);
            return true;
        }

        Double timestamp = headersType.getTimestamp();
        if (timestamp == null) {
            MaxLogger.error(AppLogFilter.class, ErrMsg.ERR_MSG_NullTimestamp);
            return true;
        }

        ActionsType actionsType = itemType.getActions();
        if (actionsType == null) {
            MaxLogger.error(AppLogFilter.class, ErrMsg.ERR_MSG_NullActionsType);
            return true;
        }

        CharSequence action = actionsType.getAction();
        if (action == null) {
            MaxLogger.error(AppLogFilter.class, ErrMsg.ERR_MSG_NullActionsName);
            return true;
        }

        String actionName = action.toString();
        if (actionName.length() == 0) {
            MaxLogger.error(AppLogFilter.class, ErrMsg.ERR_MSG_EmptyActionsName);
            return true;
        }

        Pattern pattern = Pattern.compile(FILTER_ACTION_PATTERN);
        Matcher matcher = pattern.matcher(actionName);
        if (matcher.find()) {
            return false;
        }
        return true;
    }

}
