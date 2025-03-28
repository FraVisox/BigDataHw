package BigDataHw.Homework_1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

        // Read number of partitions
        int L = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        int M = Integer.parseInt(args[3]);

        System.out.println("Command line arguments:");
        for (int i = 0; i<4; i++) {
            System.out.println(args[i]);
        }

        //TODO: read the input and transform them into instances of class Vector


        // Read input file and subdivide it into L random partitions
        JavaRDD<String> inputPoints = sc.textFile(args[0]).repartition(L).cache();
        // textFile read the file and transforms it into an RDD of strings (elements = lines).
        // Repartition forces spark to partition in L groups (at random).
        // Cache says that if Spark has to materialize it, it will store in RAM if there is space.


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long N, NA, NB;
        //TODO: calculate and store NA and NB and print them
        N = inputPoints.count(); //With this we are storing the RDD
        System.out.println("N = " + N);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //TODO: invoke the built-in Lloyd's algorithm with M iterations


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALL OBJECTIVE FUNCTIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        double standardObjective = MRComputeStandardObjective();
        double fairObjective = MRComputeFairObjective();
        System.out.println("Standard objective: "+standardObjective);
        System.out.println("Fair objective: "+fairObjective);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CALL MRPRINTSTATISTICS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        MRPrintStatistics();
    }

    public static double MRComputeStandardObjective() { //Input is an RDD and set of centroids
        return 0.0;
    }

    public static double MRComputeFairObjective() { //Input is an RDD and set of centroids
        return 0.0;
    }

    public static void MRPrintStatistics() { //Input is an RDD and set of centroids

    }
}