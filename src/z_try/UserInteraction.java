package z_try;

/**
 * Created by TAN on 2015/6/15.
 */
public class UserInteraction {

	private String mUserId;
	private float mFrequency;
	private float mEndorsement;
	
	public UserInteraction(String userId, float frequency, float endorsement) {
		mUserId = userId;
		mFrequency = frequency;
		mEndorsement = endorsement;
	}
	
	public String getUserId() {
		return mUserId;
	}
	
	public float getFrequency() {
		return mFrequency;
	}
	
	public float getEndorsement() {
		return mEndorsement;
	}	
}
