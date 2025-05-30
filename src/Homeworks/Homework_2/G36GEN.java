package Homeworks.Homework_2;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import javax.validation.constraints.NotNull;
import java.util.*;


public class G36GEN {

    private static final Random random = new Random(1);

    //Boolean definition of the groups. In case the groups were more than 2 we could use an enum.
    private final static boolean groupA = true;
    private final static boolean groupB = false;

    //Parameters used in the following: we can vary them to improve the radius
    private final static double big_radius = 1;
    private final static double small_radius = 0;
    private final static double y_B = 0;
    private final static double y_A = 100;
    private final static double gap = 500;

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
        System.out.printf(Locale.ENGLISH, "%.4f,%.4f,%c\n", coords[0], coords[1], group == groupA ? 'A' : 'B');
    }

    public static @NotNull List<Pair<double[], Boolean>> generateDataset(int N, int K) {
        //The seed is set here for reproducibility in case we need to test it more than once after creating
        random.setSeed(1);

        //Supposition: N is greater than K, and both are positive
        if (K <= 0 || K > N) {
            throw new IllegalArgumentException("K and N should be positive, and N > K");
        }

        List<Pair<double[], Boolean>> dataset = new ArrayList<>();


        // Points for the first cluster
        int points_first_cluster = N-(K-1);

        // Generate the first cluster
        dataset.addAll(generateCluster(points_first_cluster-1, 0, y_A, big_radius, groupA));
        dataset.addAll(generateCluster(1, 0, y_B, small_radius, groupB));

        // Generate K-1 points at distance gap one from the other
        double curr_x = gap;
        for (int i = 1; i<K; i++) {
            dataset.addAll(generateCluster(1, curr_x, y_B, small_radius, groupA));
            curr_x += gap;
        }

        return dataset;
    }

    // Generates one cluster of the given radius around the center
    private static List<Pair<double[], Boolean>>  generateCluster(int n, double centerX, double centerY, double radius, boolean group) {
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