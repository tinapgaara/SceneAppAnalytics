package z_try;

/**
 * Created by TAN on 2015/6/15.
 */
public class UserSimilarity {

	private String mUserId;
	private float mSimilarity;
	
	public UserSimilarity(String userId, float fSimilarity) {
		mUserId = userId;
		mSimilarity = fSimilarity;
	}
	
	public String getUserId() {
		return mUserId;
	}
	
	public float getSimilarity() {
		return mSimilarity;
	}
}

