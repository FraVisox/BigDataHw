package Homeworks.Homework_2;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.util.*;


public class G36GEN {

    private static final Random random = new Random(1);

    //Boolean definition of the groups. In case the groups were more than 2 we could use an enum.
    private final static boolean groupA = true;
    private final static boolean groupB = false;

    public static void main(String[] args) {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS.
        // Parameters are: num_points (N), num_centers (K)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_points num_centers");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //Limit the number of warnings
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Spark Configuration that defines the application.
        SparkConf conf = new SparkConf(true).setAppName("G36GEN");
        //Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read parameters
        int N = Integer.parseInt(args[0]);
        int K = Integer.parseInt(args[1]);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // GENERATE THE DATASET AND OUTPUT IT IN STANDARD OUTPUT
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        List<Pair<double[], Boolean>> dataset = generateDataset(N, K);

        for (Pair<double[], Boolean> point : dataset) {
            printPoint(point.getKey(), point.getValue());
        }

    }

    private static void printPoint(double[] coords, boolean group) {
        // EXAMPLE OUTPUT: 40.7267,-74.0345,B
        System.out.printf(Locale.ENGLISH, "%.4f,%.4f,%c\n", coords[0], coords[1], group == groupA ? 'A' : 'B');
    }

    private static @NotNull List<Pair<double[], Boolean>> generateDataset(int N, int K) {
        //Supposition: N is much larger than K
        if (K <= 0) {
            throw new IllegalArgumentException("K cannot be zero or less");
        }
        List<Pair<double[], Boolean>> dataset = new ArrayList<>();

        if (K == 1) {
            //TODO:
        }

        // Generate the two clusters near the origin
        dataset.addAll(generateCluster(N*5/12, random, 20, 120, 10, groupA));
        dataset.addAll(generateCluster(N/12, random, 20, 20, 3, groupB));

        dataset.addAll(generateCluster(N*5/12, random, 80, 120, 10, groupA));
        dataset.addAll(generateCluster(N/12, random, 80, 20, 3, groupB));

        //TODO: generate the other clusters

        return dataset;
    }

    /**
     * Generates the upper cluster near the origin with mostly group A points
     */
    private static List<Pair<double[], Boolean>>  generateCluster(int n, Random random, double centerX, double centerY, double radius, boolean group) {

        List<Pair<double[], Boolean>> answer = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double x = centerX + random.nextGaussian() * radius;
            double y = centerY + random.nextGaussian() * radius;

            double[] coords = {x,y};
            answer.add(new ImmutablePair<>(coords, group));
        }
        return answer;
    }
}