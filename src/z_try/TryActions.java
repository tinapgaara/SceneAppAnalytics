package z_try;

import maxA.io.sparkClient.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;
import static org.apache.spark.mllib.random.RandomRDDs.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by max2 on 7/31/15.
 */
public class TryActions {

    private static int number = 0;

    public static void main (String[] agrs) {

        SparkContext.setJscStartFlag();
        JavaSparkContext sc = SparkContext.getSparkContext();
        JavaDoubleRDD u = normalJavaRDD(sc, 32, 10);
        final int len = 8;
        final double threshold =0.5;
        JavaPairRDD<Tuple2<Integer, Integer>, LabeledPoint> pairs = u.mapToPair(new PairFunction<Double, Tuple2<Integer, Integer>, LabeledPoint>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, LabeledPoint> call(Double value) {
                double newVal = 0;
                if (value > threshold) {
                    newVal = 1;
                }
                Vector vector = null;
                LabeledPoint point = null;
                int id = TryActions.number / len;
                int vectorIndice = TryActions.number % len;

                if (vectorIndice < (len - 1)) {
                    int[] indices = new int[]{0, 1, 2, 3, 4, 5, 6};
                    double[] values = new double[len-1];
                    for (int i = 0; i < values.length; i++) {
                        if (i != vectorIndice) {
                            values[i] = 0;
                        } else {
                            values[i] = newVal;
                        }
                    }
                    vector = Vectors.sparse(len-1, indices, values);
                    point = new LabeledPoint(0, vector);
                } else {
                    double mLabel = newVal;
                    int[] indices = new int[]{0, 1, 2, 3, 4, 5, 6};
                    double[] values = new double[]{0, 0, 0, 0, 0, 0, 0};
                    vector = Vectors.sparse(len-1, indices, values);
                    point = new LabeledPoint(mLabel, vector);
                }

                TryActions.number ++;
                return new Tuple2<Tuple2<Integer, Integer>, LabeledPoint>(new Tuple2<Integer, Integer>(id, id), point);
            }
        });

        JavaPairRDD<Tuple2<Integer, Integer>, LabeledPoint> reducedPairs = pairs.reduceByKey(new Function2<LabeledPoint, LabeledPoint, LabeledPoint>() {
            @Override
            public LabeledPoint call(LabeledPoint labeledPoint, LabeledPoint labeledPoint2) throws Exception {
                Vector v_1 = labeledPoint.features();
                Vector v_2 = labeledPoint2.features();
                double label_1 = labeledPoint.label();
                double label_2 = labeledPoint2.label();

                int len = v_1.size();
                int[] indices = new int[]{0,1,2,3,4,5,6};
                double[] values = new double[len];
                for (int i = 0; i < v_1.size() ; i ++) {
                    values[i] = v_1.apply(i) + v_2.apply(i);
                }
                Vector newVector = Vectors.sparse(len, indices, values);
                double newLabel = label_1 + label_2;

                return new LabeledPoint(newLabel, newVector);
            }
        });

        JavaRDD<LabeledPoint> result = reducedPairs.values();
        List<LabeledPoint> res = result.collect();
        for (LabeledPoint point : res) {
            System.out.println("**********************");
            Vector v = point.features();
            double label = point.label();

            for( int i = 0 ; i < v.size() ; i ++) {
                System.out.println(i + "th value:" + v.apply(i));
            }
            System.out.println("label : "+label);
        }


        /*
        List<Tuple2<Long, Integer>> pairList = new ArrayList<Tuple2<Long, Integer>>();
        pairList.add(new Tuple2<Long, Integer>(new Long(1), 3));
        pairList.add(new Tuple2<Long, Integer>(new Long(1), 4));
        pairList.add(new Tuple2<Long, Integer>(new Long(1), 5));
        pairList.add(new Tuple2<Long, Integer>(new Long(1), 6));



        List<Tuple2<Long, Integer>> pairList_2 = new ArrayList<Tuple2<Long, Integer>>();
        pairList_2.add(new Tuple2<Long, Integer>(new Long(-1), 7));
        pairList_2.add(new Tuple2<Long, Integer>(new Long(-1), 8));

        JavaRDD<Tuple2<Long, Integer>> pairs_1 = sc.parallelize(pairList);
        JavaRDD<Tuple2<Long, Integer>> pairs_2 = sc.parallelize(pairList_2);

        pairs_1 = pairs_1.union(pairs_2);



        JavaRDD<Integer> res = pairs_1.map(new Function<Tuple2<Long,Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Long, Integer> tuple2) throws Exception {
                System.out.println(tuple2._2());
                return tuple2._2();
            }
        });

        /*
        JavaRDD<Integer> res = rddpairs.flatMap(
                new FlatMapFunction<Tuple2<Long, Integer>, Integer>() {
                    @Override
                    public Iterable<Integer> call(Tuple2<Long, Integer> tuple2) throws Exception {
                        List<Integer> res = new ArrayList<Integer>();
                        System.out.println(tuple2._2());
                        res.add(tuple2._2());

                        return res;
                    }
                });

*/
        /*
        List<Integer> resList = res.collect();
        System.out.println(resList.size());

        System.out.println(resList.get(0) + ", " + resList.get(1) + ", " + resList.get(2));
        */

    }
}
