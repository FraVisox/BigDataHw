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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;


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
                new DenseVector(new double[]{40.749035, -73.984431}),
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

    public static double MRComputeStandardObjective(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) { //Input is an RDD and set of centroids
        return 0.0;
    }

    public static double MRComputeFairObjective(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) {
        //TODO: choose the best one

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
        //ROUND 3:
        // * (group, sum, N) -> compute the mean -> (spark_dummy_key, mean)
        // * (spark_dummy_key, mean) -> take the max -> (spark_dummy_key, max)
        long start = System.nanoTime();
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
        long end = System.nanoTime();
        System.out.println("Time of reduceByKey: "+(end-start));

        System.out.println("--------------------------");

        //3 ROUNDS with partitions
        //Passages:
        //ROUND 1:
        // * (point, group) -> compute the closest centroid and squared distance -> (group, distance)
        // * (group, distance) -> for every partition: sum all the distances and take the mean -> (group, mean)
        //ROUND 2:
        // * empty
        // * (group, mean) -> take the total mean on everything -> (mean)
        //ROUND 3:
        // * empty
        // * (mean) -> take the max -> (max)
        start = System.nanoTime();
        double ans1 = rdd
                //ROUND 1:
                //map 1
                .mapToPair(x -> {
                        //Compute minimum squared distance -> (group, distance): O(1) local memory
                        double min_dist = Vectors.sqdist(x._1, centroids[0]);
                        for (int i = 1; i < centroids.length; i++) {
                            min_dist = Math.min(min_dist, Vectors.sqdist(x._1, centroids[i]));
                        }
                        return new Tuple2<>(x._2, min_dist);
                    })
                //reduce 1
                .mapPartitionsToPair(
                    //In every partition, get sum and N
                    (x) -> {
                        double sumA = 0;
                        double sumB = 0;
                        double nna = 0;
                        double nnb = 0;
                        while (x.hasNext()){
                            Tuple2<Boolean, Double> tuple = x.next();
                            if (tuple._1 == groupA) {
                                nna++;
                                sumA += tuple._2;
                            } else {
                                nnb++;
                                sumB += tuple._2;
                            }
                        }
                        ArrayList<Tuple2<Boolean, Double[]>> pairs = new ArrayList<>(2);
                        pairs.add(new Tuple2<Boolean, Double[]>(groupA, new Double[]{sumA, nna}));
                        pairs.add(new Tuple2<Boolean, Double[]>(groupB, new Double[]{sumB, nnb}));
                        return pairs.iterator();
                    })
                //ROUND 2:
                //shuffle 2
                .groupByKey()
                //reduce 2
                .map((it) -> {
                    //Finally, get the total mean
                    double sum = 0;
                    double N = 0;
                    for (Double[] el : it._2) {
                        sum += el[0];
                        N += el[1];
                    }
                    return sum/N; //We don't even use a key, as Spark will do that for us
                })
                //ROUND 3:
                //reduce 3
                .reduce(Math::max);

        end = System.nanoTime();
        System.out.println("Time of using partitions: "+(end-start));

        return ans;
    }

    public static void MRPrintStatistics(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) {
		// TASK:
		// Write a method/function MRPrintStatistics that takes in input the set U=A∪B and a set C of centroids,
		// and computes and prints the triplets (ci,NAi,NBi), for 1≤i≤K=|C|
		// where ci is the i-th centroid in C, and NAi,NBi are the numbers of points of A and B, respectively, in the cluster Ui centered in ci.

        //TODO: decide if it is needed to mantain the same order

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

        /*
        List<Tuple2<Vector, int[]>> out1 = rdd
                //ROUND 1:
                // map: (point, group) -> (index_of_center, group)
                .mapToPair(x -> {
                    double min_dist = Vectors.sqdist(x._1, centroids[0]);
                    Vector center = centroids[0];
                    for (int i = 1; i < centroids.length; i++) {
                        double d = Vectors.sqdist(x._1, centroids[i]);
                        if (d < min_dist) {
                            min_dist = d;
                            center = centroids[i];
                        }
                    }
                    int[] values = new int[2];
                    values[x._2 ? 0 : 1] = 1;
                    return new Tuple2<>(center, values);
                })
                // reduce: for every index of centers we sum up all the points of the same group
                .reduceByKey((x,y) -> {
                    return new int[] {x[0]+y[0], x[1]+y[1]};
                })
                .collect();

        int i = 0;
        for (Tuple2<Vector, int[]> tuple : out1) {
            double[] ci = tuple._1.toArray();
            int NAi = tuple._2[0];
            int NBi = tuple._2[1];
            System.out.printf(Locale.ENGLISH, "i = %d, center = (%.6f,%.6f), NA%d = %d, NB%d = %d\n", i, ci[0], ci[1], i, NAi, i, NBi);
            i++;
        }

         */

    }

}
