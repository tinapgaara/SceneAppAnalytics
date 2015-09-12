package maxA.userProfile;

/**
 * Created by TAN on 6/15/2015.
 */
public class UserAttribute {

	public static String interestSimAttibuteName = "interestSimAttribute";

	protected String mName;

	protected UserAttributeValue mValue;

	public UserAttribute(String name, UserAttributeValue value) {

		mName = name;
		mValue = value;
	}

	public String getName() {
		return mName;
	}

	public UserAttributeValue getValue() {
		return mValue;
	}

	public void release() {

		mName = null;
		if (mValue != null) {
			mValue.release();
			mValue = null;
		}
	}
}
