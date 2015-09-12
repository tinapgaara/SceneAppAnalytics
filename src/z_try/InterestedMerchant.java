package z_try;

/**
 * Created by TAN on 2015/6/15.
 */
public class InterestedMerchant {

	private String mMerchantId;
	private float mInterestDegree;
	
	public InterestedMerchant(String merchantId, float interestDegree) {
		mMerchantId = merchantId;
		mInterestDegree = interestDegree;
	}
	
	public String getMerchantId() {
		return mMerchantId;
	}
	
	public float getInterestedDegree() {
		return mInterestDegree;
	}
}

