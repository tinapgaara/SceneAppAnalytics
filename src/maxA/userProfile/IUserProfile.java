package maxA.userProfile;

import scala.Tuple2;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by TAN on 6/20/2015.
 */
public interface IUserProfile {

	public int getUserId();

	public UserAttributeValue getUserAttributeValue(String attributeName);

	/*
	public List<Tuple2<Double, Long>> getTopItems(int featureId,
			int maxNumOfItems);
	//*/

	public List<Tuple2<Double, Long>> getTopItems(String featureName,
												  int maxNumOfItems);
	public void release();

}

