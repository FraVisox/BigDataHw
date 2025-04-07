package Homeworks.Homework_1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;

import java.util.*;


public class G36HW1 {

    private final static boolean groupA = true;
    private final static boolean groupB = false;


    public static void main(String[] args) throws InterruptedException {

        //TODO: check output format on the examples

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
        //Spark Configuration that defines the application. True is used to read command line arguments
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

        //This is done locally
        JavaPairRDD<Vector, Boolean> inputPoints = data.mapToPair((x) -> {
            String[] point = x.split(",");
            double[] values = new double[point.length-1];
            for (int i = 0; i< point.length-1; i++) {
                values[i] = Double.parseDouble(point[i]);
            }
            //true if group = "A", false otherwise
            return new Tuple2<>(Vectors.dense(values), point[point.length-1].equals("A"));
        }).cache();


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long N, NA, NB;
        N = inputPoints.count();

        //A simple map phase in which we invert the key and value and then use countByKey of Spark.
        Map<Boolean, Long> counts = inputPoints.mapToPair((x) -> new Tuple2<>(x._2, x._1)).countByKey();
        NA = counts.get(groupA);
        NB = counts.get(groupB);

        System.out.println("N = "+N+", NA = "+NA+", NB = "+NB);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //Invoke the algorithm only on the points, without the group
        KMeansModel clusters = KMeans.train(inputPoints.map(x -> x._1).rdd(), K, M);

        Vector[] centers = clusters.clusterCenters();

        /* ONLY FOR DEBUG PURPOSES
        Vector[] centers = new Vector[] {
                new DenseVector(new double[]{40.749035,-73.984431}),
                new DenseVector(new double[]{40.873440,-74.192170}),
                new DenseVector(new double[]{40.693363,-74.178147}),
                new DenseVector(new double[]{40.746095,-73.830627})
        };

         */

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

        int dummy_key = 1;

        long start = System.nanoTime();

        //Get the number of points
        long count = rdd.count();

        // Calculate the sum of squared distances
        double sumOfSquaredDistances = rdd
                //ROUND 1
                //map
                .mapToPair(point -> {
                    double minDist = Vectors.sqdist(point._1, centroids[0]);
                    for (int i = 1; i < centroids.length; i++) {
                        minDist = Math.min(minDist, Vectors.sqdist(point._1, centroids[i]));
                    }
                    return new Tuple2<>(dummy_key, minDist);
                })
                //reduce of Round 1 and Round 2
                .reduceByKey(Double::sum)
                .first()._2;
        long end = System.nanoTime();
        System.out.println("Count and sum: "+(end-start));

        double ans1 = sumOfSquaredDistances/count;
        System.out.println("sum/count = "+ans1);

        //TODO: proposal which calculates both N and sum at the same time
        start = System.nanoTime();
        double[] ans = rdd
                //ROUND 1
                //map
                .mapToPair(point -> {
                    double minDist = Vectors.sqdist(point._1, centroids[0]);
                    for (int i = 1; i < centroids.length; i++) {
                        minDist = Math.min(minDist, Vectors.sqdist(point._1, centroids[i]));
                    }
                    return new Tuple2<>(dummy_key, new double[] {minDist, 1});
                })
                //reduce of Round 1 and Round 2
                .reduceByKey((x,y) ->  new double[] {x[0]+y[0], x[1]+y[1]})
                .first()._2;
        end = System.nanoTime();
        System.out.println("New proposal: "+(end-start));

        return ans[0]/ans[1];
    }

    public static double MRComputeFairObjective(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) {
        if (centroids.length == 0)
            return 0;

        //3 ROUNDS with reduceByKey
        //Passages:
        //ROUND 1:
        // * (point, group) -> compute the closest centroid and squared distance -> (group, min_distance)
        // * (group, min_distance) -> for every partition: sum all the distances and the number of points -> (group, sum_l, N_l)
        //ROUND 2:
        // * empty
        // * (group, sum_l, N_l) -> for every group, get the resulting sum and N -> (group, sum, N)
        //ROUND 3: note that spark_dummy_key is a dummy key given implicitly by Spark
        // * (group, sum, N) -> compute the mean -> (spark_dummy_key, mean)
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
                .reduceByKey((x,y) -> {
                    return new double[] {x[0]+y[0], x[1]+y[1]};
                })
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
		// TASK:
		// Write a method/function MRPrintStatistics that takes in input the set U=A∪B and a set C of centroids,
		// and computes and prints the triplets (ci,NAi,NBi), for 1≤i≤K=|C|
		// where ci is the i-th centroid in C, and NAi,NBi are the numbers of points of A and B, respectively, in the cluster Ui centered in ci.

        List<Tuple2<Integer, int[]>> out = rdd
                //ROUND 1:
                // map: (point, group) -> (index_of_center, group)
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
			.reduceByKey((x,y) -> {
				return new int[] {x[0]+y[0], x[1]+y[1]};
			})
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
