package Homeworks.Homework_2;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static Homeworks.Homework_2.G36GEN.generateDataset;
import static Homeworks.Homework_2.G36HW2.MRComputeFairObjective;
import static Homeworks.Homework_2.G36HW2.MRFairLloyd;

public class TestGEN {
    public static void main(String[] args) { //TODO: test

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Spark Configuration that defines the application.
        SparkConf conf = new SparkConf(true).setAppName("G36HW2");
        //Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<CompletableFuture<Void>> futures = new ArrayList<>();


        for (int K = 2; K < 15; K++) {
            double mean = 0;
            int tot = 0;
            //for (int N = K+1; N < 10000; N += 100) {
            for (int N = Math.max(2*K-3,K+1); N < 2*K+5; N++) {
                int finalK = K;
                int finalN = N;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    func(finalK, finalN,sc);
                }, executorService);
                futures.add(future);
            }
            //System.out.println("K = "+K+" has mean ratio of "+(mean/tot));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executorService.shutdown();
    }

    private static void func(int K, int N, JavaSparkContext sc) {
        List<Pair<double[], Boolean>> dataset = generateDataset(N, K);
        if (dataset.size() != N) {
            System.out.print("------------------\nERROR: size is "+dataset.size());
            System.out.println("--------------------");
        }

        //TODO: K=3 and N=8 or 9
        //TODO: K=4 and N=12
        //TODO: K=5 and N=7 to 13 and 16

        // Test of radius:
        JavaPairRDD<Vector, Boolean> inputPoints = sc.parallelize(dataset).mapToPair(x -> {
            return new Tuple2<>(new DenseVector(x.getKey()), x.getValue());
        });

        // Standard
        RDD<Vector> onlyPoints = inputPoints.map(x -> x._1).rdd();
        KMeansModel clusters = KMeans.train(onlyPoints, K, 200);
        Vector[] c_stand = clusters.clusterCenters();

        double stand_rad = MRComputeFairObjective(inputPoints, c_stand);

        // Fair
        Map<Boolean, Long> counts = inputPoints.mapToPair((x) -> new Tuple2<>(x._2, x._1)).countByKey();
        long NA = counts.getOrDefault(true, 0L);
        long NB = counts.getOrDefault(false, 0L);
        if (NA == 0L || NB == 0L) {
            System.out.println("NA="+NA+", NB="+NB+" with K="+K+" and N="+N);
        }
        Vector[] c_fair = MRFairLloyd(inputPoints, NA, NB, K, 200);

        double fair_rad = MRComputeFairObjective(inputPoints, c_fair);

        // Division
        double division = stand_rad/fair_rad;
        System.out.println("Il rapporto per K="+K+",N="+N+" è "+division);
    }
}
