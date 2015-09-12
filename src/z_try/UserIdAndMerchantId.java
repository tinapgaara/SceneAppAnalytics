package maxA.userProfile.impl.linearRegression;

import maxA.io.AppLogRecord;
import maxA.util.MaxLogger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by max2 on 7/19/15.
 */
public class UserIdAndMerchantId extends java.lang.Object implements Serializable {

    private long mUserId;
    private long mMerchantId;

    public UserIdAndMerchantId(long userId, long merchantId) {

        mUserId = userId;
        mMerchantId =merchantId;
    }

    public long getUserId() {

        return mUserId;
    }

    public long getMerchantId() {

        return mMerchantId;
    }

    @Override
    public boolean equals(Object obj) { // obj is null ???
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }

        UserIdAndMerchantId other = (UserIdAndMerchantId) obj;

        return ( (mUserId == other.mUserId) && (mMerchantId == other.mMerchantId) );

    }

    public List<UserIdAndMerchantId> map2UserIdAndMerchantIds(AppLogRecord appLogRecord) {
        List<UserIdAndMerchantId> userIdAndMerchantIdList = new ArrayList<UserIdAndMerchantId>();

        long userId = appLogRecord.getUserId();
        int actionName = appLogRecord.getActionName();

        if (userId == new Long(-1)) {
            return userIdAndMerchantIdList;
        }

        if (actionName == AppLogRecord.ACTION_interest) {
        // Todo: map to interestId to merchantIds
            List<Integer> merchantIds = new ArrayList<Integer>();
            merchantIds.add(1);
            merchantIds.add(2);

            if (merchantIds != null) {
                for (Integer merchantId : merchantIds) {
                    UserIdAndMerchantId userIdAndMerchantId = new UserIdAndMerchantId(userId, merchantId);
                    userIdAndMerchantIdList.add(userIdAndMerchantId);
                }
            }
        }
        else if ( (actionName == AppLogRecord.ACTION_endorse) || (actionName == AppLogRecord.ACTION_ask) ) {
            long merchantId = appLogRecord.getContextValue(AppLogRecord.CONTEXT_NAME_merchantId);//Todo: prob: assume 1-1
            if (merchantId != -1) {
                UserIdAndMerchantId userIdAndMerchantId = new UserIdAndMerchantId(userId, merchantId);
                userIdAndMerchantIdList.add(userIdAndMerchantId);
            }
            MaxLogger.info(LR_TrainData_UserMerchant.class, "----------------endorse, ask [map2UserIdAndMerchantIds] merchantId:" + merchantId + " , userId:" + userId + "----------------");
        }
        else {
            long merchantId = appLogRecord.getActionId();
            UserIdAndMerchantId userIdAndMerchantId = new UserIdAndMerchantId(userId, merchantId);
            userIdAndMerchantIdList.add(userIdAndMerchantId);

            MaxLogger.info(LR_TrainData_UserMerchant.class, "----------------[map2UserIdAndMerchantIds] merchantId:" + merchantId + " , userId:" + userId + "----------------");
        }
        MaxLogger.info(LR_TrainData_UserMerchant.class, "----------------[map2UserIdAndMerchantIds] size:" + userIdAndMerchantIdList.size()+ "----------------");
        return userIdAndMerchantIdList;
    }

}
