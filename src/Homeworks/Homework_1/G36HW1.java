package Homeworks.Homework_1;

import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.K;
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
import org.sparkproject.dmg.pmml.True;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;


public class G36HW1 {
    public static void main(String[] args) {

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

        //TODO: read the input and transform them into instances of class Vector.
        // DO WE NEED TO PUT REPARTITION ON TEXT FILE OR INPUT POINTS? In teoria è uguale, dopo pure inputPoints è su 5 partitions

        // Read input file and subdivide it into L random partitions
        JavaRDD<String> data = sc.textFile(args[0]).repartition(L);

        //This should be done locally
        JavaPairRDD<Vector, Boolean> inputPoints = data.mapToPair((x) -> {
            String[] point = x.split(",");
            double[] values = new double[point.length-1];
            for (int i = 0; i< point.length-1; i++) {
                values[i] = Double.parseDouble(point[i]);
            }
            return new Tuple2<>(Vectors.dense(values), point[point.length-1].equals("A"));
        }).cache();
        // textFile read the file and transforms it into an RDD of strings (elements = lines).
        // Repartition forces spark to partition in L groups (at random).
        // Cache says that if Spark has to materialize it, it will store in RAM if there is space.


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long N, NA = 782, NB = 230;
        //TODO: calculate and store NA and NB and print them
        N = inputPoints.count(); //With this we are storing the RDD

        //Map<Boolean, Long> counts = inputPoints.countByKey();
        //NA = counts.get(true);
        //NB = counts.get(false);

        System.out.println("N = "+N+", NA = "+NA+", NB = "+NB);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //TODO: invoke the built-in Lloyd's algorithm with M iterations and K clusters. IS IT RIGHT?
        KMeansModel clusters = KMeans.train(inputPoints.map(x -> x._1).rdd(), K, M);

        //Vector[] centers = clusters.clusterCenters();

        Vector[] centers = new Vector[] {
                new DenseVector(new double[]{40.749035, -73.984431}),
                new DenseVector(new double[]{40.873440,-74.192170}),
                new DenseVector(new double[]{40.693363,-74.178147}),
                new DenseVector(new double[]{40.746095,-73.830627})
        };


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALL OBJECTIVE FUNCTIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        double standardObjective = MRComputeStandardObjective(inputPoints, centers);
        double fairObjective = MRComputeFairObjective(inputPoints, centers);
        System.out.printf("Delta(U,C) = %.6f \n", standardObjective/N);
        System.out.printf("Phi(A,B,C) = %.6f \n", fairObjective);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALL MRPRINTSTATISTICS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        MRPrintStatistics(inputPoints, clusters.clusterCenters());
    }

    public static double MRComputeStandardObjective(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) { //Input is an RDD and set of centroids
        return 0.0;
    }

    public static double MRComputeFairObjective(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) { //Input is an RDD and set of centroids
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
        if (centroids.length == 0)
            return 0;

        //2 ROUNDS with reduce by key
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
                //reduce by key itself uses partitions
                .reduceByKey((x,y) -> {
                    return new double[] {x[0]+y[0], x[1]+y[1]};
                })
                //ROUND 2
                //map
                .map((it) -> {
                    return it._2[0]/it._2[1];
                })
                //reduce
                .reduce(Math::max);
        long end = System.nanoTime();
        System.out.println("Time: "+(end-start));

        System.out.println("----------------------------------");

        //2 ROUNDS without using the partitions
        start = System.nanoTime();
        double ans2 = rdd
                //ROUND 1:
                //map
                .mapToPair(x -> {
                    //Compute minimum squared distance -> (group, distance): O(N) local memory
                    double min_dist = Vectors.sqdist(x._1, centroids[0]);
                    for (int i = 1; i < centroids.length; i++) {
                        min_dist = Math.min(min_dist, Vectors.sqdist(x._1, centroids[i]));
                    }
                    return new Tuple2<>(x._2, min_dist);
                })
                //reduce
                .groupByKey().map(
                        (it) -> {
                            //O(N) space
                            double sum = 0;
                            long n = 0;
                            for (double d : it._2) {
                                sum += d;
                                n++;
                            }
                            return sum/n;
                        }
                )
                //ROUND 2:
                //reduce
                .reduce(Math::max);

        end = System.nanoTime();
        System.out.println("Time: "+(end-start));
        System.out.println("--------------------------");

        //2 ROUNDS
        start = System.nanoTime();
        double ans3 = rdd
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
                            if (tuple._1) {
                                nna++;
                                sumA += tuple._2;
                            } else {
                                nnb++;
                                sumB += tuple._2;
                            }
                        }
                        ArrayList<Tuple2<Boolean, Double[]>> pairs = new ArrayList<>(2);
                        pairs.add(new Tuple2<Boolean, Double[]>(true, new Double[]{sumA, nna}));
                        pairs.add(new Tuple2<Boolean, Double[]>(false, new Double[]{sumB, nnb}));
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
        System.out.println("Time: "+(end-start));

        System.out.println("Ans "+ans+" Ans2 "+ans2+" Ans3 "+ans3);
        return ans3;
    }

    public static void MRPrintStatistics(JavaPairRDD<Vector, Boolean> rdd, Vector[] centroids) { //Input is an RDD and set of centroids
    }

}