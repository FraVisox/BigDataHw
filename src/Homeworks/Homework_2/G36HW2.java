package Homeworks.Homework_2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.Tuple3;

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

        //This is done to store the input data so that time profiling is done in the right way
        N = inputPoints.count();
        //A simple map phase in which we invert the key and value and then use countByKey of Spark.
        Map<Boolean, Long> counts = inputPoints.mapToPair((x) -> new Tuple2<>(x._2, x._1)).countByKey();
        NA = counts.get(groupA);
        NB = counts.get(groupB);

        System.out.println("N = "+N+", NA = "+NA+", NB = "+NB);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // STANDARD LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        long start, end;

        //First be sure that the RDD is stored: we don't want to count the time to take out only the points
        RDD<Vector> onlyPoints = inputPoints.map(x -> x._1).rdd().cache();
        onlyPoints.count();

        //Invoke the algorithm only on the points, without the group and compute the centers (also this last part is measured).
        start = System.currentTimeMillis();
        KMeansModel clusters = KMeans.train(onlyPoints, K, M);
        Vector[] c_stand = clusters.clusterCenters();
        end = System.currentTimeMillis();

        long c_stand_time = end-start;

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // FAIR LLOYD'S ALGORITHM INVOCATION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //Invoke our fair implementation: here inputPoints has already been stored since we counted N above
        start = System.currentTimeMillis();
        Vector[] c_fair = MRFairLloyd(inputPoints, NA, NB, K, M);
        end = System.currentTimeMillis();

        long c_fair_time = end-start;

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // FIND VALUES OF OBJECTIVE FUNCTIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //Here inputPoints has already been stored since we counted N above
        start = System.currentTimeMillis();
        double standObjective = MRComputeFairObjective(inputPoints, c_stand);
        end = System.currentTimeMillis();

        long stand_obj_time = end-start;

        //Here inputPoints has already been stored since we counted N above
        start = System.currentTimeMillis();
        double fairObjective = MRComputeFairObjective(inputPoints, c_fair);
        end = System.currentTimeMillis();

        long fair_obj_time = end-start;

        System.out.printf(Locale.ENGLISH, "Fair objective with Standard Centers %.4f \n", standObjective);
        System.out.printf(Locale.ENGLISH, "Fair objective with Fair Centers %.4f \n", fairObjective);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PRINT RUNNING TIMES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        System.out.printf(Locale.ENGLISH, "Time to compute standard centers = %d ms \n", c_stand_time);
        System.out.printf(Locale.ENGLISH, "Time to compute fair centers = %d ms \n", c_fair_time);
        System.out.printf(Locale.ENGLISH, "Time to compute objective with standard centers = %d ms \n", stand_obj_time);
        System.out.printf(Locale.ENGLISH, "Time to compute objective with fair centers = %d ms \n", fair_obj_time);
    }

	public static Vector add(Vector vec1, Vector vec2) {
        if (vec1.size() != vec2.size()) throw new IllegalArgumentException("Vectors must have the same dimension");
        double[] result = new double[vec1.size()];
        for (int i = 0; i < vec1.size(); i++) {
            result[i] = vec1.apply(i) + vec2.apply(i);
        }
        return Vectors.dense(result);
    }

    public static Vector[] MRFairLloyd(JavaPairRDD<Vector, Boolean> rdd, long NA, long NB, int K, int M) {
        //Initialize using kmeans||
        KMeansModel clusters = KMeans.train(rdd.map(x -> x._1).rdd(), K, 0);
        Vector[] centers = clusters.clusterCenters();
        //Executes M iterations of the loop
		for (int iter = 0; iter < M; iter++) {
			// assign each point to the closest center, O(nk)
			JavaRDD<Tuple3<Vector, Boolean, Integer>> partitioned = rdd.map(pair -> {
				Vector x = pair._1;
				int c = -1;
				double best_d = Double.POSITIVE_INFINITY;
				for (int i = 0; i < K; i++) {
					double d = Vectors.sqdist(x, centers[i]);
					if (d < best_d) {
						best_d = d;
						c = i;
					}
				}
				return new Tuple3<>(x, pair._2, c);
			}).cache();

			// for each group,partition compute the size of the intersection (group ∩ U_i)
			// for each group,partition compute the sum of points in the intersection
			Map<Tuple2<Boolean,Integer>, Tuple2<Long, Vector>> groupStats = partitioned
				.mapToPair(t -> new Tuple2<>(new Tuple2<>(t._2(), t._3()), new Tuple2<>(1L, t._1())))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, add(x._2, y._2)))
				.collectAsMap();

			// aggregate relevant global statistics in O(k)
			long[] countA = new long[K], countB = new long[K];
			double[] alpha = new double[K], beta = new double[K];
			Vector[] muA = new Vector[K], muB = new Vector[K];
			double[] ell = new double[K];

            double[] null_coords = new double[centers[0].size()];
            for (int coord=0;coord< centers[0].size();coord++) {
                null_coords[coord] = 0;
            }

            Vector null_vector = new DenseVector(null_coords);

			for (int i = 0; i < K; i++) {
				Tuple2<Long, Vector> statsA = groupStats.getOrDefault(new Tuple2<>(groupA, i), new Tuple2<>(0L, null_vector));
				Tuple2<Long, Vector> statsB = groupStats.getOrDefault(new Tuple2<>(groupB, i), new Tuple2<>(0L, null_vector));
				countA[i] = statsA._1; countB[i] = statsB._1;
				Vector sumA = statsA._2, sumB = statsB._2;
				alpha[i] = (double) countA[i] / NA;
				beta[i] = (double) countB[i] / NB;
                if (countA[i] != 0) {
                    BLAS.scal(1.0 / countA[i], sumA);
                }
                if (countB[i] != 0) {
                    BLAS.scal(1.0 / countB[i], sumB);
                }
				muA[i] = sumA;
				muB[i] = sumB;
				ell[i] = Math.sqrt(Vectors.sqdist(muA[i], muB[i]));
			}

			// compute distance from centers, O(n)
			Map<Boolean, Double> delta = partitioned
				.mapToPair(t -> new Tuple2<>(new Tuple2<>(t._2(), t._3()), t._1()))
				.mapToPair(pair -> {
					Boolean group = pair._1._1;
					Vector[] muGroup = group ? muA : muB;
					Vector mu = muGroup[pair._1._2];
					double d = Vectors.sqdist(pair._2, mu);
					return new Tuple2<>(group, d);
				})
				.reduceByKey(Double::sum)
				.collectAsMap();
			double fixedA = delta.getOrDefault(groupA, 0.0) / NA;
			double fixedB = delta.getOrDefault(groupB, 0.0) / NB;

			// select next centroids, O(kT)
			double[] xs = computeVectorX(fixedA, fixedB, alpha, beta, ell, K);
			for (int i = 0; i < K; i++) {
				if (countA[i] == 0) {
					centers[i] = muB[i];
				} else if (countB[i] == 0) {
					centers[i] = muA[i];
				} else {
					double x = xs[i], l = ell[i];
					Vector ma = muA[i], mb = muB[i];
					BLAS.scal((l - x) / l, ma);
					BLAS.scal(x / l, mb);
					centers[i] = add(ma, mb);
				}
			}
		}
        //Returns the set of C centroids
        return centers;
    }

    public static double[] computeVectorX(double fixedA, double fixedB, double[] alpha, double[] beta, double[] ell, int K) {
        double gamma = 0.5;
        double[] xDist = new double[K];
        double fA, fB;
        double power = 0.5;
        int T = 10;
        for (int t=1; t<=T; t++){
            fA = fixedA;
            fB = fixedB;
            power = power/2;
            for (int i=0; i<K; i++) {
                double temp = (1-gamma)*beta[i]*ell[i]/(gamma*alpha[i]+(1-gamma)*beta[i]);
                xDist[i]=temp;
                fA += alpha[i]*temp*temp;
                temp=(ell[i]-temp);
                fB += beta[i]*temp*temp;
            }
            if (fA == fB) {break;}
            gamma = (fA > fB) ? gamma+power : gamma-power;
        }
        return xDist;
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

        return rdd
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
    }

}
