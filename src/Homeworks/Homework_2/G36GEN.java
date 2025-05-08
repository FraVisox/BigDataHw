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

        //TODO: find a way to generate N points in R^2 such that if used in G36HW2 they show a great difference between the distances

        List<Pair<double[], Boolean>> dataset = generateDataset(N, K);

        for (Pair<double[], Boolean> point : dataset) {
            printPoint(point.getKey(), point.getValue());
        }

    }

    private static void printPoint(double[] coords, boolean group) {
        // EXAMPLE OUTPUT: 40.7267,-74.0345,B
        System.out.printf(Locale.ENGLISH, "%.4f,%.4f,%c\n", coords[0], coords[1], group == groupA ? 'A' : 'B');
    }


    private static final int DIMENSIONS = 2; // 2D space for visualization
    private static final double CLUSTER_RADIUS_B = 2.0; // Radius for dense group B clusters
    private static final double CLUSTER_RADIUS_A = 0.5; // Radius for group A clusters
    private static final double GROUP_AB_DISTANCE = 8.0; // Distance between paired A and B clusters
    private static final double SCATTERED_CLUSTER_SPREAD = 2.0; // Spread for scattered B clusters
    private static final double CLUSTER_SEPARATION = 100.0; // Base separation between cluster centers

    private static final double ratio = 9.0/10;


    private static @NotNull List<Pair<double[], Boolean>> generateDataset(int N, int K) {
        //Supposition: N is much larger than K
        if (K <= 0) {
            throw new IllegalArgumentException("K cannot be zero or less");
        }
        List<Pair<double[], Boolean>> dataset = new ArrayList<>();

        // Generate cluster centers: first for scattered B clusters, then for paired A-B clusters
        List<double[]> clusterCenters = generateClusterCenters(K);

        if (K == 1) {
            dataset.addAll(generateDenseCluster(
                    clusterCenters.get(0),
                    (int) Math.ceil(N*ratio)-1, // More B points
                    CLUSTER_RADIUS_B,
                    groupB
            ));

            dataset.addAll(generateDenseCluster(
                    offsetCenter(clusterCenters.get(0), GROUP_AB_DISTANCE),
                    (N- (int) Math.ceil(N*ratio))+1, // Less A points
                    CLUSTER_RADIUS_A,
                    groupA
            ));
            return dataset;
        }

        int pointsPerCluster = N / K;

        // 1. Generate K-2 scattered clusters with only group B points
        for (int clusterId = 2; clusterId < K; clusterId++) {
            dataset.addAll(generateScatteredCluster(
                    clusterCenters.get(clusterId),
                    pointsPerCluster,
                    SCATTERED_CLUSTER_SPREAD
            ));
        }

        int remaining_points = N-pointsPerCluster*(K-2);

        int first_clust_points = remaining_points/2;

        //TODO: questi dovresti metterli in centro al cerchio, e messi come nella immagine

        // 2. Generate two special paired clusters (group B dense balls with nearby group A balls)
        // First paired cluster
        dataset.addAll(generateDenseCluster(
                clusterCenters.get(0),
                (int) Math.ceil(first_clust_points*ratio)-1, // More B points
                CLUSTER_RADIUS_B,
                groupB
        ));

        dataset.addAll(generateDenseCluster(
                offsetCenter(clusterCenters.get(0), GROUP_AB_DISTANCE),
                first_clust_points-(int) Math.ceil(first_clust_points*ratio)+1, // Less A points
                CLUSTER_RADIUS_A,
                groupA
                ));

        remaining_points = remaining_points - first_clust_points;

        // Second paired cluster
        dataset.addAll(generateDenseCluster(
                clusterCenters.get(1),
                (int) Math.ceil(remaining_points*ratio)-1,
                CLUSTER_RADIUS_B,
                groupB
                ));

        dataset.addAll(generateDenseCluster(
                offsetCenter(clusterCenters.get(1), GROUP_AB_DISTANCE),
                remaining_points - (int) Math.ceil(remaining_points*ratio)+1,
                CLUSTER_RADIUS_A,
                groupA
                ));

        return dataset;
    }

    /**
     * Generate k cluster centers far apart from each other.
     */
    private static List<double[]> generateClusterCenters(int k) {
        //TODO: why in circle? No sense bro

        List<double[]> centers = new ArrayList<>();

        // Place centers in a roughly circular arrangement to maximize separation
        for (int i = 0; i < k; i++) {
            double angle = (2 * Math.PI * i) / k;
            double[] center = new double[DIMENSIONS];
            center[0] = CLUSTER_SEPARATION * Math.cos(angle);
            center[1] = CLUSTER_SEPARATION * Math.sin(angle);
            centers.add(center);
        }

        return centers;
    }

    /**
     * Generate a dense cluster of points around a center with specified radius.
     */
    private static List<Pair<double[], Boolean>> generateDenseCluster(double[] center, int numPoints, double radius, boolean isGroupA) {
        List<Pair<double[], Boolean>> cluster = new ArrayList<>();

        for (int i = 0; i < numPoints; i++) {
            double[] coordinates = new double[DIMENSIONS];

            // Generate random direction and distance within radius
            double angle = random.nextDouble() * 2 * Math.PI;
            double distance = random.nextDouble() * radius;

            // Convert to Cartesian coordinates and add to center
            coordinates[0] = center[0] + distance * Math.cos(angle);
            coordinates[1] = center[1] + distance * Math.sin(angle);

            cluster.add(new ImmutablePair<>(coordinates, isGroupA));
        }

        return cluster;
    }

    /**
     * Generate a scattered cluster of group B points around a center.
     */
    private static List<Pair<double[], Boolean>> generateScatteredCluster(
            double[] center, int numPoints, double spread) {
        List<Pair<double[], Boolean>> cluster = new ArrayList<>();

        for (int i = 0; i < numPoints; i++) {
            double[] coordinates = new double[DIMENSIONS];

            // Generate random offsets from center with specified spread
            for (int d = 0; d < DIMENSIONS; d++) {
                coordinates[d] = center[d] + (random.nextDouble() - 0.5) * 2 * spread;
            }

            cluster.add(new ImmutablePair<>(coordinates, false)); // All points are group B
        }

        return cluster;
    }

    /**
     * Calculate a new center offset from original by specific distance.
     */
    private static double[] offsetCenter(double[] center, double distance) {
        double[] newCenter = new double[center.length];

        // Offset in random direction
        double angle = random.nextDouble() * 2 * Math.PI;
        newCenter[0] = center[0] + distance * Math.cos(angle);
        newCenter[1] = center[1] + distance * Math.sin(angle);

        return newCenter;
    }

}