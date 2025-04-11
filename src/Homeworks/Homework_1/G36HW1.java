package Homeworks.Homework_1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;

import java.util.*;


public class G36HW1 {

    //Boolean definition of the groups. In case the groups were more than 2 we could use an enum.
    private final static boolean groupA = true;
    private final static boolean groupB = false;

    public static void main(String[] args) {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS.
        // Parameters are: <file_path>, num_partitions (L), num_centers (K), num_iterations (M)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: file_path num_partitions num_centers num_iterations");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //Limit the number of warnings
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Spark Configuration that defines the application.
        SparkConf conf = new SparkConf(true).setAppName("G36HW1");
        //Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read parameters
        int L = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        int M = Integer.parseInt(args[3]);

        //Print parameters
        System.out.println("Input file = "+args[0]+", L = "+L+", K = "+K+", M = "+M);

        // Read input file and subdivide it into L random partitions
        JavaRDD<String> data = sc.textFile(args[0]).repartition(L);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // RDD CREATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //1 ROUND, each line is computed separately.
        JavaPairRDD<Vector, Boolean> inputPoints = data.mapToPair((x) -> {
            String[] point = x.split(",");
            double[] values = new double[point.length-1];
            for (int i = 0; i< point.length-1; i++) {
                values[i] = Double.parseDouble(point[i]);
            }
            //true if group = "A", false if group = "B"
            return new Tuple2<>(Vectors.dense(values), point[point.length-1].equals("A"));
        }).cache();


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long N, NA, NB;

        //A simple map phase in which we invert the key and value and then use countByKey of Spark.
        Map<Boolean, Long> counts = inputPoints.mapToPair((x) -> new Tuple2<>(x._2, x._1)).countByKey();
        NA = counts.get(groupA);
        NB = counts.get(groupB);
        //This is done to prevent from doing another useless round
        N = NA+NB;

        System.out.println("N = "+N+", NA = "+NA+", NB = "+NB);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //Invoke the algorithm only on the points, without the group and compute the centers
        KMeansModel clusters = KMeans.train(inputPoints.map(x -> x._1).rdd(), K, M);
        Vector[] centers = clusters.clusterCenters();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALL OBJECTIVE FUNCTIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        double standardObjective = MRComputeStandardObjective(inputPoints, centers);
        double fairObjective = MRComputeFairObjective(inputPoints, centers);

        System.out.printf(Locale.ENGLISH, "Delta(U,C) = %.6f \n", standardObjective);
        System.out.printf(Locale.ENGLISH, "Phi(A,B,C) = %.6f \n", fairObjective);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALL MRPRINTSTATISTICS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        MRPrintStatistics(inputPoints, centers);
    }

    public static double MRComputeStandardObjective(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) {
        if (centroids.length == 0) {
            return 0.0;
        }

        //2 ROUNDS with reduceByKey
        //Passages:
        //ROUND 1:
        // * (point, group) -> compute the closest centroid and squared distance -> (dummy_key, (min_distance,1))
        // * (dummy_key, (min_distance,1)) -> for every partition: sum all the distances and the number of points -> (dummy_key, (sum_l, N_l))
        //ROUND 2:
        // * empty
        // * (dummy_key, (sum_l, N_l)) ->  get the resulting sum and N -> (dummy_key, (sum, N))

        //Dummy key used in the MR algorithm
        final int dummy_key = 1;

        double[] ans = rdd
                //ROUND 1
                //map
                .mapToPair(point -> {
                    double minDist = Vectors.sqdist(point._1, centroids[0]);
                    for (int i = 1; i < centroids.length; i++) {
                        minDist = Math.min(minDist, Vectors.sqdist(point._1, centroids[i]));
                    }
                    return new Tuple2<>(dummy_key, new double[] {minDist,1});
                })
                //reduce of Round 1 and Round 2
                .reduceByKey((x,y) -> new double[] {x[0]+y[0], x[1]+y[1]})
                .first()._2;

        return ans[0]/ans[1];
    }

    public static double MRComputeFairObjective(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) {
        if (centroids.length == 0)
            return 0;

        //3 ROUNDS with reduceByKey
        //Passages:
        //ROUND 1:
        // * (point, group) -> compute the closest centroid and squared distance -> (group, (min_distance, 1))
        // * (group, (min_distance, 1)) -> for every partition: sum all the distances and the number of points -> (group, (sum_l, N_l))
        //ROUND 2:
        // * empty
        // * (group, (sum_l, N_l)) -> for every group, get the resulting sum and N -> (group, (sum, N))
        //ROUND 3: note that spark_dummy_key is a dummy key given implicitly by Spark. This round could also be done without map reduce
        // * (group, (sum, N)) -> compute the mean -> (spark_dummy_key, mean)
        // * (spark_dummy_key, mean) -> take the max -> (spark_dummy_key, max)

        double ans = rdd
                //ROUND 1
                //map
                .mapToPair(x -> {
                    double min_dist = Vectors.sqdist(x._1, centroids[0]);
                    for (int i = 1; i < centroids.length; i++) {
                        min_dist = Math.min(min_dist, Vectors.sqdist(x._1, centroids[i]));
                    }
                    return new Tuple2<>(x._2, new double[] {min_dist, 1});
                })
                //reduce of ROUND 1 and ROUND 2:
                //reduceByKey itself uses partitions
                .reduceByKey((x,y) -> new double[] {x[0]+y[0], x[1]+y[1]})
                //ROUND 3
                //map
                .map((it) ->
                    it._2[0]/it._2[1]
                )
                //reduce
                .reduce(Math::max);

        return ans;
    }

    public static void MRPrintStatistics(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) {
		if (centroids.length == 0) {
            return;
        }
        //For the analysis, see the .docx

        List<Tuple2<Integer, int[]>> out = rdd
                //ROUND 1:
                // map: (point, group) -> (index_of_center, (1,0)) or (index_of_center, (0,1))
                .mapToPair(x -> {
				double min_dist = Vectors.sqdist(x._1, centroids[0]);
				int index = 0;
				for (int i = 1; i < centroids.length; i++) {
					double d = Vectors.sqdist(x._1, centroids[i]);
					if (d < min_dist) {
						min_dist = d;
						index = i;
					}
				}
				int[] values = new int[2];
				values[x._2 ? 0 : 1] = 1;
				return new Tuple2<>(index, values);
			})
            // reduce: for every index of centers we sum up all the points of the same group
			.reduceByKey((x,y) -> new int[] {x[0]+y[0], x[1]+y[1]})
			.sortByKey()
            .collect();

        for (Tuple2<Integer, int[]> tuple : out) {
            int i = tuple._1;
            double[] ci = centroids[i].toArray();
            int NAi = tuple._2[0];
            int NBi = tuple._2[1];
            System.out.printf(Locale.ENGLISH, "i = %d, center = (%.6f,%.6f), NA%d = %d, NB%d = %d\n", i, ci[0], ci[1], i, NAi, i, NBi);
        }
    }

}