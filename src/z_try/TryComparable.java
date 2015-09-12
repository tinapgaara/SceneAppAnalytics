package z_try;

import maxA.recommender.IMerchantRecommendation;
import maxA.recommender.impl.MerchantRecommendation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by max2 on 8/4/15.
 */
public class TryComparable {

    public static class MerchantRecommendationComparators implements Comparator<IMerchantRecommendation> {

        @Override
        public int compare(IMerchantRecommendation s1,IMerchantRecommendation s2){
            if ( (s1 != null) && (s2 != null) ) {
                if (s1.getLikeDegree() > s2.getLikeDegree()) {
                    return -1;
                }
                else if (s1.getLikeDegree() < s2.getLikeDegree()) {
                    return 1;
                }
            }
            return 0;
        }
    }

    public static void main(String[] args) {
        long m1 = 10;
        long m2 = 12;
        double v1 = 0.2;
        double v2 = 0.3;
        MerchantRecommendation rec = new MerchantRecommendation(m1, v1);
        MerchantRecommendation rec_2 = new MerchantRecommendation(m2, v2);

        List<MerchantRecommendation> list = new ArrayList<MerchantRecommendation>();
        list.add(rec);
        list.add(rec_2);

        Collections.sort(list, new MerchantRecommendationComparators());

        for(int i = 0; i < list.size(); i ++) {
            System.out.println("---------------- [recMerchants2User reorder], "+ list.get(i).getMerchantId() +"th merchantId:"
                    + list.get(i).getLikeDegree());
        }

    }
}
