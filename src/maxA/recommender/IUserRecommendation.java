package maxA.recommender;

/**
 * Created by TAN on 2015/6/15.
 */
public interface IUserRecommendation {

	public String getUserId();
	
	public float getWeight();
	
	public float getConfidenceDegree();
	
	public IExplaination getExplaination();
	
}
