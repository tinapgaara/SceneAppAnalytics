package maxA.recommender;

/**
 * Created by TAN on 2015/6/15.
 */
public interface IMerchantRecommendation {

	public long getMerchantId();
	
	public double getLikeDegree();
	
	public double getConfidenceDegree();
	
	public IExplaination getExplaination();
	
}

