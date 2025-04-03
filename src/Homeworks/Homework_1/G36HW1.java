package Homeworks.Homework_1;

import algebra.lattice.Bool;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Array;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;

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
        JavaPairRDD<Boolean, Vector> inputPoints = data.mapToPair((x) -> {
            String[] point = x.split(",");
            double[] values = new double[point.length-1];
            for (int i = 0; i< point.length-1; i++) {
                values[i] = Double.parseDouble(point[i]);
            }
            return new Tuple2<Boolean, Vector>(point[point.length-1].equals("A"), Vectors.dense(values));
        }).cache();
        // textFile read the file and transforms it into an RDD of strings (elements = lines).
        // Repartition forces spark to partition in L groups (at random).
        // Cache says that if Spark has to materialize it, it will store in RAM if there is space.


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long N, NA = 0, NB = 0;
        //TODO: calculate and store NA and NB and print them
        N = inputPoints.count(); //With this we are storing the RDD

        Map<Boolean, Long> counts = inputPoints.countByKey();
        NA = counts.get(true);
        NB = counts.get(false);

        System.out.println("N = "+N+", NA = "+NA+", NB = "+NB);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //TODO: invoke the built-in Lloyd's algorithm with M iterations and K clusters. IS IT RIGHT?
        KMeansModel clusters = KMeans.train(inputPoints.map(x -> x._2).rdd(), K, M);


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALL OBJECTIVE FUNCTIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        double standardObjective = MRComputeStandardObjective(inputPoints, clusters.clusterCenters());
        double fairObjective = MRComputeFairObjective(inputPoints, clusters.clusterCenters());
        System.out.println("Delta(U,C) = "+standardObjective);
        System.out.println("Phi(A,B,C) = "+fairObjective);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALL MRPRINTSTATISTICS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        MRPrintStatistics(inputPoints, clusters.clusterCenters());
    }

    public static double MRComputeStandardObjective(JavaPairRDD<Boolean, Vector> rdd, Vector[] centroids) { //Input is an RDD and set of centroids
        return 0.0;
    }

    public static double MRComputeFairObjective(JavaPairRDD<Boolean, Vector> rdd, Vector[] centroids) { //Input is an RDD and set of centroids
        return 0.0;
    }

    public static void MRPrintStatistics(JavaPairRDD<Boolean, Vector> rdd, Vector[] centroids) { //Input is an RDD and set of centroids

    }

}