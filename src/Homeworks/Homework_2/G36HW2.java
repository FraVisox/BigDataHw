package Homeworks.Homework_2;

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

import java.util.List;
import java.util.Locale;
import java.util.Map;


public class G36HW2 {

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
        SparkConf conf = new SparkConf(true).setAppName("G36HW2");
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
        // STANDARD LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        long start, end;

        //Invoke the algorithm only on the points, without the group and compute the centers
        start = System.currentTimeMillis();
        KMeansModel clusters = KMeans.train(inputPoints.map(x -> x._1).rdd(), K, M);
        Vector[] c_stand = clusters.clusterCenters();
        end = System.currentTimeMillis();

        long c_stand_time = end-start;

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // FAIR LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        start = System.currentTimeMillis();
        Vector[] c_fair = MRFairLloyd(inputPoints, K, M);
        end = System.currentTimeMillis();

        long c_fair_time = end-start;

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // FIND VALUES OF OBJECTIVE FUNCTIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        start = System.currentTimeMillis();
        double standObjective = MRComputeFairObjective(inputPoints, c_stand);
        end = System.currentTimeMillis();

        long stand_obj_time = end-start;

        start = System.currentTimeMillis();
        double fairObjective = MRComputeFairObjective(inputPoints, c_fair);
        end = System.currentTimeMillis();

        long fair_obj_time = end-start;

        System.out.printf(Locale.ENGLISH, "Phi(A,B,Cstand) = %.6f \n", standObjective);
        System.out.printf(Locale.ENGLISH, "Phi(A,B,Cfair) = %.6f \n", fairObjective);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PRINT RUNNING TIMES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //TODO: measure in the right way, not with currentTimeMillis()
        System.out.printf(Locale.ENGLISH, "Cstand running time = %d \n", c_stand_time);
        System.out.printf(Locale.ENGLISH, "Cfair running time = %d \n", c_fair_time);
        System.out.printf(Locale.ENGLISH, "Stand obj running time = %d \n", stand_obj_time);
        System.out.printf(Locale.ENGLISH, "Fair obj running time = %d \n", fair_obj_time);


    }

    public static Vector[] MRFairLloyd(JavaPairRDD<Vector, Boolean> rdd, long K, long M) {
        //Initialize using kmeans||
        //Executes M iterations of the loop
        //Returns the set of C centroids
        return null;
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

}