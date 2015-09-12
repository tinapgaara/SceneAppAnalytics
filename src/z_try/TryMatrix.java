package z_try;

import maxA.io.sparkClient.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Function3;
import scala.Tuple2;
import scala.Unit;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by max2 on 7/28/15.
 */
public class TryMatrix {

    public static MatrixEntry combine (MatrixEntry m1, MatrixEntry m2) {
        long rowId = m1.i();
        long colId = m2.j();
        return new MatrixEntry(rowId, colId, (m1.value() + m2.value()) );
    }

    public static void main (String[] agrs) {
        int[] colPtrs = new int[]{0, 0, 0, 3};//colPtrs the index corresponding to the start of a new column (if not transposed) param
        // first col contains :3-4
        // second col contains: 4-5
        // third col contains: 5-6
        int[] indices = new int[]{1,2};
        double[] values = new double[]{1,1};
        Vector ve = Vectors.sparse(3,indices, values);

        IndexedRow row = new IndexedRow(0, ve);

//        double[] elements = new double[]{1,2,3,4,5,6};
        double[] elements = new double[]{1,1,1};
        int[] rowIndices = new int[]{1,1,1};
        SparseMatrix matrix = new SparseMatrix(3, 3, colPtrs, rowIndices, elements);

        List<MatrixEntry> entries = new ArrayList<MatrixEntry>();
        MatrixEntry entry = new MatrixEntry(0,1,2);
        entries.add(entry);

        JavaSparkContext sc = SparkContext.getSparkContext();
        JavaRDD<MatrixEntry> matrixEntriesRDD_1 = sc.parallelize(entries);

        List<MatrixEntry> entries_2 = new ArrayList<MatrixEntry>();
        MatrixEntry entry_2 = new MatrixEntry(0,0,1);
        entries_2.add(entry_2);
        JavaRDD<MatrixEntry> matrixEntriesRDD_2 = sc.parallelize(entries_2);

        matrixEntriesRDD_1 = matrixEntriesRDD_1.union(matrixEntriesRDD_2);

        JavaPairRDD<Tuple2<Long, Long>, MatrixEntry> pairs = matrixEntriesRDD_1.mapToPair(
            new PairFunction<MatrixEntry, Tuple2<Long, Long>, MatrixEntry>() {
                public Tuple2<Tuple2<Long, Long>, MatrixEntry> call(MatrixEntry entry) {
                    System.out.print("---------------" + entry.value());
                    return new Tuple2<Tuple2<Long, Long>, MatrixEntry>(new Tuple2<Long, Long>(entry.i(), entry.j()), entry);
                }
            });

        JavaPairRDD<Tuple2<Long, Long>, MatrixEntry> reducedPairs = pairs.reduceByKey(
                new Function2<MatrixEntry, MatrixEntry, MatrixEntry>() {
                    public MatrixEntry call(MatrixEntry matrixEntry, MatrixEntry matrixEntry2) throws Exception {
                        return combine(matrixEntry, matrixEntry2);
                    }
                });

        JavaRDD<MatrixEntry> newEntries = reducedPairs.map(
                new Function<Tuple2<Tuple2<Long,Long>,MatrixEntry>, MatrixEntry>() {
                    public MatrixEntry call (Tuple2<Tuple2<Long,Long>,MatrixEntry> tuple) {
                        return tuple._2();
                    }
                });

        if (newEntries == null) {
            System.out.println("null");
        }
        else {

            List<MatrixEntry> v = newEntries.collect();
            System.out.println("not null, [" + v.get(1).i()+ "," + v.get(1).j()+"] :" + v.get(1).value());
        }
    }
}
