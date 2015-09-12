package maxA.common.redis;

import avro.Give;
import avro.GiveLogSchemaHelper;
import maxA.common.Constants;
import maxA.util.ErrMsg;
import maxA.util.MaxLogger;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by max2 on 8/7/15.
 */
public class RedisHelper {

    private static RedisHelper m_instance = null;

    private RedisHelper() {
        // nothing to do here
    }

    public static RedisHelper getInstance() {
        if (m_instance == null) {
            m_instance = new RedisHelper();
        }

        return m_instance;
    }

    public String getRatingByGiveId(long userId, long merchantId, long giveId) {

        String key = "GIVE:" + userId + ":" + merchantId + ":null:" + giveId;
        String giveJsonStr = RedisCache.INSTANCE.getFromKey(key);

        Give give = GiveLogSchemaHelper.convertStr2AvroGive(giveJsonStr);
        CharSequence attrName = give.getAttrName();
        CharSequence attrValue = give.getAttrValue();

        if ( (attrName == null) || (attrValue == null)) {
            MaxLogger.error(RedisHelper.class,
                    ErrMsg.ERR_MSG_IllegalGive);

            return null;
        }
        else if ( ! attrName.equals("Rating") ) {
            MaxLogger.info(RedisHelper.class,
                            "----------------[getRatingByGiveId]: This Give doesn't contain rating ----------------");

            return null;
        }

        return attrValue.toString();
    }

    public Vector getUserInterestVector(long userId) {

        String key = "USER:INTEREST:" + userId;

        Set<String> interestIds = RedisCache.INSTANCE.getAllFromSet(key);

        int setSize = interestIds.size();

        int interestNum = Constants.REDIS_INTERTEST_NUMBER;
        int[] indices = new int[setSize];
        double[] values = new double[setSize];

        int tempIndex = 0;
        String strPattern  = "INTEREST:(\\d+)";
        Pattern pattern = Pattern.compile(strPattern);

        for (String strId : interestIds) {
            Matcher matcher = pattern.matcher(strId);

            if (matcher.find()) {
                int interestId = Integer.parseInt(matcher.group(1));

                indices[tempIndex] = interestId;
                values[tempIndex] = 1;

                tempIndex++;
            }
            else {
                MaxLogger.error(RedisHelper.class,
                                ErrMsg.ERR_MSG_IllegalInterestValueInRedis);

                return null;
            }
        }

        Vector res = Vectors.sparse(interestNum, indices, values);

        return res;
    }

    public List<Long> getAllOtherUsersByInterest(long userId) {

        Set<String> keys = RedisCache.INSTANCE.getAllKeys("USER:INTEREST:");
        String strPattern  = "INTEREST:(\\d+)";
        Pattern pattern = Pattern.compile(strPattern);

        List<Long> res = new ArrayList<Long>();

        for (String key : keys) {

            Matcher matcher = pattern.matcher(key);
            if (matcher.find()) {
                key = matcher.group(1);

                Long longKey = Long.parseLong(key);
                if (longKey != userId) {
                    res.add(longKey);
                }
            }
            else {
                MaxLogger.error(RedisHelper.class,
                                ErrMsg.ERR_MSG_IllegalInterestValueInRedis);

                continue;
            }
        }

        return res;
    }
}
