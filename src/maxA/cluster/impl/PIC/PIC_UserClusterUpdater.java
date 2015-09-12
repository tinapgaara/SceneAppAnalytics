package maxA.cluster.impl.PIC;

import maxA.cluster.IUserClusterModel;
import maxA.cluster.impl.GenericUserClusterUpdater;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by max2 on 7/30/2015.
 */
public class PIC_UserClusterUpdater extends GenericUserClusterUpdater {

    private static PIC_UserClusterUpdater m_instance = null;

    public static PIC_UserClusterUpdater getInstance(IUserClusterModel userClusterModel) {

        if (m_instance == null) {
            m_instance = new PIC_UserClusterUpdater(userClusterModel);
        }
        return m_instance;
    }

    private PIC_UserClusterUpdater(IUserClusterModel userClusterModel) {
        super(userClusterModel);
    }

    @Override
    public void updateClustersByModel(int clusterNum, CoordinateMatrix userUserMatrix) {
        // map each entry in matrix to a Tuple3<Long, Long, Double>, so can run PIC
        JavaPairRDD<Tuple2<Long, Long>, Double> pairs = userUserMatrix.entries().toJavaRDD().mapToPair(
            new PairFunction<MatrixEntry, Tuple2<Long, Long>, Double>() {
                @Override
                public Tuple2<Tuple2<Long, Long>, Double> call (MatrixEntry entry) {
                    long rowId = entry.i();
                    long colId = entry.j();
                    double val = entry.value();

                    if (rowId > colId) {
                        long temp = rowId;
                        rowId = colId;
                        colId = temp;
                    }
                    Tuple2<Long, Long> key = new Tuple2<Long, Long>(rowId, colId);
                    return new Tuple2<Tuple2<Long, Long>, Double>(key, (val / 2) );
                }
            });

        JavaPairRDD<Tuple2<Long, Long>, Double> reducedPairs = pairs.reduceByKey(
            new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double d_1, Double d_2) throws Exception {
                    return (d_1 + d_2);
                }
            });

        JavaRDD<Tuple3<Long, Long, Double>> clusterData = reducedPairs.flatMap(new FlatMapFunction<Tuple2<Tuple2<Long, Long>, Double>, Tuple3<Long, Long, Double>>() {
            public Iterable<Tuple3<Long, Long, Double>> call(Tuple2<Tuple2<Long, Long>, Double> tuple2) throws Exception {
                Tuple2<Long, Long> indexs = tuple2._1();
                long rowId = indexs._1();
                long colId = indexs._2();
                double val = tuple2._2();

                List<Tuple3<Long, Long, Double>> list = new ArrayList<Tuple3<Long, Long, Double>>();
                if (val > 0) {
                    list.add(new Tuple3<Long, Long, Double>(rowId, colId, val));
                }
                return list;
            }
        });

        mUserClusterModel.cluster(clusterNum, new PIC_ClusterData(clusterData));
    }
}
