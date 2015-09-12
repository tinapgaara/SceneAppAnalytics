package maxA.io;


import avro.AppLogs.*;
import maxA.common.AppLogConstants;
import maxA.common.Constants;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by TAN on 7/5/2015.
 */
public class AppLogRecord implements IRecord {

    public static final String CONTEXT_NAME_rating = "Rating";
    public static final String CONTEXT_NAME_queue = "Queue";
    public static final String CONTEXT_NAME_atmosphere = "Atmosphere";
    public static final String CONTEXT_NAME_specials = "Specials";

    public static final String CONTEXT_NAME_merchantId = "merchantID";
    public static final String CONTEXT_NAME_giverId = "giverID";
    public static final String CONTEXT_NAME_planId = "planID";

    public static final int ACTION_give = 1001;
    public static final int ACTION_reply = 1002;
    public static final int ACTION_ask = 1003;
    public static final int ACTION_enter = 1004;
    public static final int ACTION_interest = 1005;
    public static final int ACTION_add = 1006;
    public static final int ACTION_bookmark = 1007;
    public static final int ACTION_unbookmark = 1008;
    public static final int ACTION_endorse = 1009;
    public static final int ACTION_vote = 1010;
    public static final int ACTION_leave = 1011;

    public static final int ACTION_VAL_merchant = 2001;
    public static final int ACTION_VAL_merchantDetails = 2002;
    public static final int ACTION_VAL_rating = 2003;
    public static final int ACTION_VAL_queue = 2004;
    public static final int ACTION_VAL_atmosphere = 2005;
    public static final int ACTION_VAL_specials = 2006;
    public static final int ACTION_VAL_true =2007;
    public static final int ACTION_VAL_false = 2008;
    public static final int ACTION_VAL_add = 2009;
    public static final int ACTION_VAL_remove = 2010;
    public static final int ACTION_VAL_people = 2011;
    public static final int ACTION_VAL_plan = 2012;

    public static final long CONTEXT_VAL_good = 3001;
    public static final long CONTEXT_VAL_bad = 3002;
    public static final long CONTEXT_VAL_short = 3003;
    public static final long CONTEXT_VAL_medium = 3004;
    public static final long CONTEXT_VAL_long = 3005;
    public static final long CONTEXT_VAL_relaxed = 3006;
    public static final long CONTEXT_VAL_lively = 3007;
    public static final long CONTEXT_VAL_buzzing = 3008;
    public static final long CONTEXT_VAL_trendy = 3009;
    public static final long CONTEXT_VAL_chic = 3010;
    public static final long CONTEXT_VAL_formal = 3011;
    public static final long CONTEXT_VAL_offbeat = 3012;
    public static final long CONTEXT_VAL_inspiring = 3013;
    public static final long CONTEXT_VAL_romantic = 3014;

    private long mUserId;

    private int mActionName; // map String to Integer
    private int mActionValue; //default: -1
    private long mActionId;

    public double mTimestamp;
    public double mLat, mLng;

    private Map<String, Long> mContexts;

    public AppLogRecord(AppLogs appLogs) {

         parseAppLog(appLogs);
    }

    public long getUserId() {

        return mUserId;
    }

    public int getActionName() {

        return mActionName;
    }

    public long getActionId() {

        return mActionId;
    }

    public int getActionValue() {

        return mActionValue;
    }

    public double getTimestamp() {

        return mTimestamp;
    }

    public Map<String, Long> getContexts() {

        return mContexts;
    }

    public long getContextValue(String contextName) {

        if (mContexts == null) {
            return -1;
        }

        Long result = mContexts.get(contextName);
        if (result == null) {
            return -1;
        }

        return result.longValue();
    }

    public boolean contextContains(String contextName) {
        return ( (mContexts != null)
                && mContexts.keySet().contains(contextName) );
    }

    private void parseAppLog(AppLogs appLogs) {

        LogItemType itemType = appLogs.getLogItem();
        HeadersType headerType = itemType.getHeaders();

        mUserId = headerType.getUserId();
        // For debug information
        MaxLogger.debug(AppLogRecord.class, "---------------- [parseAppLog] userID: " + mUserId);
        //

        mTimestamp = headerType.getTimestamp();
        mLat = headerType.getUserLat();
        mLng = headerType.getUserLong();
        // For debug information
        MaxLogger.debug(AppLogRecord.class, "---------------- [parseAppLog] timestamp: " + mTimestamp + ", lat:" + mLat
                + ", lng:" + mLng);
        //

        // Actions
        ActionsType actionsType = itemType.getActions();
        CharSequence actionNameChar = actionsType.getAction();
        CharSequence actionValueChar = actionsType.getValue();
        if (actionValueChar == null) {
            MaxLogger.info(AppLogRecord.class, ErrMsg.ERR_MSG_NullActionsValue);
        }

        String actionName = actionNameChar.toString();
        String actionValue = actionValueChar.toString();
        mActionName = map_strActionName2Int(actionName);
        mActionValue = map_strActionValue2Int(actionValue);

        CharSequence actionIdChar = actionsType.getId();
        if (actionIdChar == null) {
            MaxLogger.info(AppLogRecord.class, ErrMsg.ERR_MSG_NullActionsId);
            return;
        }

        String actionStrId = actionIdChar.toString();
        if (actionStrId.length() == 0) {
            mActionId = -1;
        }
        else {
            // TODO: change to long when AppLogs format has been corrected
            // long actionLong = Long.parseLong(actionStrId)
            if (actionStrId.contains("INTEREST:")) {
                String strPattern  = "INTEREST:(\\d+)";
                Pattern pattern = Pattern.compile(strPattern);
                Matcher matcher = pattern.matcher(actionStrId);
                if (matcher.find()) {
                    mActionId = Integer.parseInt(matcher.group(1));
                }

            }
            else {
                double actionDouble = Double.parseDouble(actionStrId);
                mActionId = (int) actionDouble;
            }
        }
        // For debug information
        MaxLogger.debug(AppLogRecord.class, "---------------- [parseAppLog] actionName: " + actionName + ", actionValue: " + actionValue
                        + ", actionID: " + mActionId);
        //

        // Contexts
        List<ContextType> contexts = itemType.getContext();
        if ( (contexts == null) || (contexts.isEmpty()) ) {
            MaxLogger.info(AppLogRecord.class,
                    ErrMsg.msg(ErrMsg.ERR_MSG_IllegalContextList,
                            (contexts == null) ? "NULL" : "Empty"));
        }
        else {
            if (mContexts == null) {
                mContexts = new HashMap<String, Long>();
            }

            for (ContextType context : contexts) {
                String contextName = context.getName().toString();
                String contextValue = context.getValue().toString();

                long nContextValue = map_strContextValue2Int(contextValue);

                // For debug information
                MaxLogger.debug(AppLogRecord.class, "---------------- [parseAppLog] contextName: " + contextName + ", contextValue:" + nContextValue);
                //

                mContexts.put(contextName, nContextValue);
            }
        }
    }

    private int map_strActionName2Int(String actionName) {

        int res = -1;
        if (actionName == null) {
            return res;
        }

        if (actionName.equals(AppLogConstants.ACTION_Str_give)) {
            res =  AppLogRecord.ACTION_give;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_reply)) {
            res = AppLogRecord.ACTION_reply;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_bookmark)) {
            res = AppLogRecord.ACTION_bookmark;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_unbookmark)) {
            res = AppLogRecord.ACTION_unbookmark;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_ask)) {
            res = AppLogRecord.ACTION_ask;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_enter)) {
            res = AppLogRecord.ACTION_enter;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_vote)) {
            res = AppLogRecord.ACTION_vote;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_endorse)) {
            res = AppLogRecord.ACTION_endorse;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_interest)) {
            res = AppLogRecord.ACTION_interest;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_add)) {
            res = AppLogRecord.ACTION_add;
        }
        else if (actionName.equals(AppLogConstants.ACTION_Str_leave)) {
            res = AppLogRecord.ACTION_leave;
        }

        return res;
    }

    private int map_strActionValue2Int(String actionValue) {

        int res = -1;
        if (actionValue == null) {
            return res;
        }
        if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_specials)) {
            res =  AppLogRecord.ACTION_VAL_specials;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_queue)) {
            res = AppLogRecord.ACTION_VAL_queue;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_atmosphere)) {
            res = AppLogRecord.ACTION_VAL_atmosphere;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_merchant)) {
            res =  AppLogRecord.ACTION_VAL_merchant;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_merchantDetails)) {
            res = AppLogRecord.ACTION_VAL_merchantDetails;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_true)) {
            res = AppLogRecord.ACTION_VAL_true;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_false)) {
            res = AppLogRecord.ACTION_VAL_false;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_add)) {
            res = AppLogRecord.ACTION_VAL_add;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_remove)) {
            res = AppLogRecord.ACTION_VAL_remove;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_people)) {
            res = AppLogRecord.ACTION_VAL_people;
        }
        else if (actionValue.equals(AppLogConstants.ACTION_VAL_Str_plan)) {
            res = AppLogRecord.ACTION_VAL_plan;
        }

        return res;
    }

    private long map_strContextValue2Int(String contextValue) {

        long res = -1;
        if ( (contextValue == null) || (contextValue.isEmpty()) ) {
            return res;
        }

        if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_good)) {
            res =  AppLogRecord.CONTEXT_VAL_good;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_bad)) {
            res = AppLogRecord.CONTEXT_VAL_bad;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_short)) {
            res = AppLogRecord.CONTEXT_VAL_short;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_medium)) {
            res = AppLogRecord.CONTEXT_VAL_medium;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_long)) {
            res = AppLogRecord.CONTEXT_VAL_long;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_relaxed)) {
            res = AppLogRecord.CONTEXT_VAL_relaxed;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_lively)) {
            res = AppLogRecord.CONTEXT_VAL_lively;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_buzzing)) {
            res = AppLogRecord.CONTEXT_VAL_buzzing;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_trendy)) {
            res = AppLogRecord.CONTEXT_VAL_trendy;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_chic)) {
            res = AppLogRecord.CONTEXT_VAL_chic;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_formal)) {
            res = AppLogRecord.CONTEXT_VAL_formal;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_offbeat)) {
            res = AppLogRecord.CONTEXT_VAL_offbeat;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_inspiring)) {
            res = AppLogRecord.CONTEXT_VAL_inspiring;
        }
        else if (contextValue.equals(AppLogConstants.CONTEXT_VAL_Str_romantic)) {
            res = AppLogRecord.CONTEXT_VAL_romantic;
        }
        else {
            double d = Double.parseDouble(contextValue);
            res = (long) d;
            // res = Integer.parseInt(contextValue);
        }

        return res;
    }

    public List<Tuple2<Long, Long>> map2UserIdAndMerchantIds() {

        List<Tuple2<Long, Long>> userIdAndMerchantIdList = new ArrayList<Tuple2<Long, Long>>();

        long userId = this.getUserId();
        int actionName = this.getActionName();

        if (userId == -1) {
            return userIdAndMerchantIdList;
        }

        Long userID = new Long(userId);

        if (actionName == AppLogRecord.ACTION_interest) {

            long interestId = this.getActionId();

            List<Long> merchantIDs = Constants.interest2Mapping.get(interestId);

            if (merchantIDs != null) {
                for (Long merchantID : merchantIDs) {
                    Tuple2<Long, Long> userIdAndMerchantId = new Tuple2<Long, Long>(userID, merchantID);
                    userIdAndMerchantIdList.add(userIdAndMerchantId);
                }
            }
        }
        else if ( (actionName == AppLogRecord.ACTION_endorse) || (actionName == AppLogRecord.ACTION_ask) ) {

            long merchantId = this.getContextValue(AppLogRecord.CONTEXT_NAME_merchantId);
            if (merchantId != -1) {
                Tuple2<Long, Long> userIdAndMerchantId = new Tuple2<Long, Long>(userID, merchantId);
                userIdAndMerchantIdList.add(userIdAndMerchantId);
            }

            // For debug information
            MaxLogger.debug(AppLogRecord.class,
                            "----------------[map2UserIdAndMerchantIds endorse, ask ] merchantId:" + merchantId + " , userId:" + userId + "----------------");
            //
        }
        else {

            long merchantID = this.getActionId();
            Tuple2<Long, Long> userIdAndMerchantId = new Tuple2<Long, Long>(userID, merchantID);
            userIdAndMerchantIdList.add(userIdAndMerchantId);

            // For debug information
            MaxLogger.debug(AppLogRecord.class,
                            "----------------[map2UserIdAndMerchantIds] merchantId:" + merchantID + " , userId:" + userId + "----------------");
            //
        }

        return userIdAndMerchantIdList;
    }

    public AppLogRecord(long userId, long actionId,
                        String actionName, String actionValue,
                        double lat, double lng, double timestamp,
                        String contextName, String contextValue,
                        String contextName_2, String contextValue_2) {

        mUserId = userId;
        mActionId = actionId;

        mActionName = map_strActionName2Int(actionName);
        mActionValue = map_strActionValue2Int(actionValue);
        mLat = lat;
        mLng = lng;

        if ((contextName != null) && (contextValue != null)) {
            mContexts = new HashMap<String, Long>();
            mContexts.put(contextName, map_strContextValue2Int(contextValue));// rating = good;

            if ((contextName_2 != null) && (contextValue_2 != null)) {
                mContexts.put(contextName_2, map_strContextValue2Int(contextValue_2));
            }
        }

        if (timestamp == 0) {
            mTimestamp = System.currentTimeMillis();
        } else {
            mTimestamp = timestamp;
        }
    }

    private void generateDataRandomly() {

        mUserId = 12;

        Random randomInt = new Random();
        int randomNum = randomInt.nextInt(27);

        String actionName = null;
        String actionValue = null;
        String contextName = null;
        String contextValue = null;
        MaxLogger.debug(AppLogRecord.class, "---------------- [generateDataRandomly]: generate case " + randomNum);

        switch(randomNum) {
            case 0:// give rating good
                actionName = "give";
                actionValue = null;
                contextName = "rating";
                contextValue = "good";
                break;
            case 1:// give rating bad
                actionName = "give";
                actionValue = null;
                contextName = "rating";
                contextValue = "bad";
                break;
            case 2:// give queue short
                actionName = "give";
                actionValue = null;
                contextName = "queue";
                contextValue = "short";
                break;
            case 3:// give queue medium
                actionName = "give";
                actionValue = null;
                contextName = "queue";
                contextValue = "medium";
                break;
            case 4:// give queue long
                actionName = "give";
                actionValue = null;
                contextName = "queue";
                contextValue = "long";
                break;
            case 5:// give atmosphere relaxed
                actionName = "give";
                actionValue = null;
                contextName = "atmosphere";
                contextValue = "relaxed";
                break;
            case 6:// give atmosphere formal
                actionName = "give";
                actionValue = null;
                contextName = "atmosphere";
                contextValue = "formal";
                break;
            case 7:// reply rating good
                actionName = "reply";
                actionValue = null;
                contextName = "rating";
                contextValue = "good";
                break;
            case 8:// reply rating bad
                actionName = "reply";
                actionValue = null;
                contextName = "rating";
                contextValue = "bad";
                break;
            case 9:// reply queue short
                actionName = "reply";
                actionValue = null;
                contextName = "queue";
                contextValue = "short";
                break;
            case 10:// reply queue medium
                actionName = "reply";
                actionValue = null;
                contextName = "queue";
                contextValue = "medium";
                break;
            case 11:// reply queue long
                actionName = "reply";
                actionValue = null;
                contextName = "queue";
                contextValue = "long";
                break;
            case 12:// reply atmosphere relaxed
                actionName = "reply";
                actionValue = null;
                contextName = "atmosphere";
                contextValue = "relaxed";
                break;
            case 13:// reply atmosphere formal
                actionName = "reply";
                actionValue = null;
                contextName = "atmosphere";
                contextValue = "formal";
                break;
            case 14:// bookmark merchant
                actionName = "bookmark";
                actionValue = "merchant";
                contextName = null;
                contextValue = null;
                break;
            case 15:// unbookmark merchant
                actionName = "unbookmark";
                actionValue = "merchant";
                contextName = null;
                contextValue = null;
                break;
            case 16:// ask specials
                actionName = "ask";
                actionValue = "specials";
                contextName = "merchantID";
                contextValue = "71";
                break;
            case 17:// ask queue
                actionName = "ask";
                actionValue = "queue";
                contextName = "merchantID";
                contextValue = "71";
                break;
            case 18:// ask atmosphere
                actionName = "ask";
                actionValue = "atmosphere";
                contextName = "merchantID";
                contextValue = "71";
                break;
            case 19:// enter merchant
                actionName = "enter";
                actionValue = "merchant";
                contextName = null;
                contextValue = null;
                break;
            case 20:// enter merchantDetails
                actionName = "enter";
                actionValue = "merchantDetails";
                contextName = null;
                contextValue = null;
                break;
            case 21:// vote true
                actionName = "vote";
                actionValue = "true";
                contextName = "planID";
                contextValue = "12";
                break;
            case 22:// vote false
                actionName = "vote";
                actionValue = "false";
                contextName = "planID";
                contextValue = "12";
                break;
            case 23:// endorse true
                actionName = "endorse";
                actionValue = "true";
                contextName = "merchantID";
                contextValue = "71";
                break;
            case 24:// endorse false
                actionName = "endorse";
                actionValue = "false";
                contextName = "merchantID";
                contextValue = "71";
                break;
            case 25:// interest add
                actionName = "interest";
                actionValue = "add";
                contextName = null;
                contextValue = null;
                break;
            case 26:// interest remove
                actionName = "interest";
                actionValue = "remove";
                contextName = null;
                contextValue = null;
                break;
            case 27:// add merchant
                actionName = "add";
                actionValue = "merchant";
                contextName = "planID";
                contextValue = "12";
                break;
        }

        mActionName = map_strActionName2Int(actionName);//give
        mActionValue = map_strActionValue2Int(actionValue);
        mActionId = 13;

        mTimestamp = 1436803045;
        mLat = 12.12; mLng = 12.12;

        if ((contextName != null) && (contextValue != null) ) {
            if (mContexts == null) {
                mContexts = new HashMap<String, Long>();
            }
            mContexts.put(contextName, map_strContextValue2Int(contextValue));// rating = good;
        }
    }

    private void generateUserUserEndorseDataRandomly() {

        mUserId = 12;

        Random randomInt = new Random();
        int randomNum = randomInt.nextInt(5);

        String actionName = null;
        String actionValue = null;
        String contextName = null;
        String contextValue = null;
        String contextName_2 = null;
        String contextValue_2 = null;
        MaxLogger.debug(AppLogRecord.class, "---------------- [generateUserUserEndorseDataRandomly]: generate case " + randomNum);

        switch(randomNum) {
            case 0:// give rating good
                actionName = "endorse";
                actionValue = "true";
                contextName = "giverID";
                contextValue = "41";
                contextName_2 = "merchantID";
                contextValue_2 = "71";
                break;

            case 1:
                actionName = "endorse";
                actionValue = "true";
                contextName = "giverID";
                contextValue = "40";
                contextName_2 = "merchantID";
                contextValue_2 = "71";
                break;

            case 2:
                actionName = "endorse";
                actionValue = "true";
                contextName = "giverID";
                contextValue = "42";
                contextName_2 = "merchantID";
                contextValue_2 = "71";
                break;

            case 3:
                actionName = "endorse";
                actionValue = "false";
                contextName = "giverID";
                contextValue = "43";
                contextName_2 = "merchantID";
                contextValue_2 = "71";
                break;

            case 4:
                actionName = "endorse";
                actionValue = "true";
                contextName = "giverID";
                contextValue = "43";
                contextName_2 = "merchantID";
                contextValue_2 = "71";
                break;
        }

        mActionName = map_strActionName2Int(actionName);//give
        mActionValue = map_strActionValue2Int(actionValue);
        mActionId = 13;

        mTimestamp = System.currentTimeMillis();
        mLat = 12.12;
        mLng = 12.12;

        if ((contextName != null) && (contextValue != null)) {
            if (mContexts == null) {
                mContexts = new HashMap<String, Long>();
            }
            mContexts.put(contextName, map_strContextValue2Int(contextValue));// rating = good;

            if ((contextName_2 != null) && (contextValue_2 != null)) {
                mContexts.put(contextName_2, map_strContextValue2Int(contextValue_2));
            }
        }
    }

}
