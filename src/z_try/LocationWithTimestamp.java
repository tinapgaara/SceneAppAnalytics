package z_try;

/**
 * Created by TAN on 2015/6/15.
 */
public class LocationWithTimestamp {

	private double mLongitude, mLatitude;
	private long mTimestamp;
	
	public LocationWithTimestamp(double longitude, double latitude, long timestamp) {
		mLongitude = longitude;
		mLatitude = latitude;	
		mTimestamp = timestamp;
	}
	
	public double getLongitude() {
		return mLongitude;
	}

	public double getLatitude() {
		return mLatitude;
	}
	
	public long getTimestamp() {
		return mTimestamp;
	}
}
