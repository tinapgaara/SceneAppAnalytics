package z_try;

/**
 * Created by TAN on 2015/6/15.
 */
public class LikeMerchant {

	private String mMerchantId;
	private float mLikeDegree;
	
	public LikeMerchant(String merchantId, float likeDegree) {
		mMerchantId = merchantId;
		mLikeDegree = likeDegree;
	}
	
	public String getMerchantId() {
		return mMerchantId;
	}
	
	public float getLikeDegree() {
		return mLikeDegree;
	}
}
