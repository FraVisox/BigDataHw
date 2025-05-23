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

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static Homeworks.Homework_2.G36GEN.generateDataset;
import static Homeworks.Homework_2.G36HW2.MRComputeFairObjective;
import static Homeworks.Homework_2.G36HW2.MRFairLloyd;

public class TestGEN {
    public static void main(String[] args) {

        /* TODO: some of the ones that have a smaller radius than expected
        NA=5, NB=3 with K=3 and N=8
        Standard centers: [[196.26320978548227,65.88382329937855], [999.779930709384,93.16510509514706], [3.358338659000033,68.16727341427953]]
        Fair centers: [[101.10824457483328,114.33373903464854], [1000.6094038286633,21.126290228929435], [98.51330386964904,19.71735767900953]]
        Fair objective with Standard Centers 3239.4818
        Fair objective with Fair Centers 9035.0797
        Il rapporto per K=3,N=8 è 0.35854490391357324

        OR

        NA=6, NB=3 with K=3 and N=9
        Standard centers: [[196.33078457015176,86.92633769989567], [1000.3971287636259,86.61284533663355], [0.4111608586551938,84.05935483459962]]
        Fair centers: [[99.66158120296598,18.790784601940565], [1000.6905110313505,119.91483621073436], [999.8103642281768,20.008863588431932]]
        Fair objective with Standard Centers 4460.8339
        Fair objective with Fair Centers 12928.8995
        Il rapporto per K=3,N=9 è 0.3450281229044172

        OR

        NA=6, NB=4 with K=4 and N=10
        Standard centers: [[195.85023441479424,93.12185105100461], [999.4678091340274,68.00046615130844], [1199.7391605470843,69.70927575725331], [3.358338659000033,68.16727341427953]]
        Fair centers: [[1098.6880442301385,117.54403260753179], [-1.0854322899236157,19.439011558221424], [200.43327777666656,19.236910487727485], [1100.5189254509733,20.165709301029963]]
        Fair objective with Standard Centers 3164.3802
        Fair objective with Fair Centers 9849.3351
        Il rapporto per K=4,N=10 è 0.3212785604749941

        OR

        NA=6, NB=5 with K=5 and N=11
        Standard centers: [[1199.4678091340274,68.00046615130844], [3.358338659000033,68.16727341427953], [2000.0078173709849,86.58207270818055], [999.2015877996207,70.24136781377929], [196.26320978548227,65.88382329937855]]
        Fair centers: [[1100.5263485822152,20.080647877378773], [7.8079052009447745,116.95908696496569], [1998.8419825869864,22.986273977415728], [198.11481653411522,20.13385189326166], [-1.0912278829447088,19.375459863593377]]
        Fair objective with Standard Centers 2681.2891
        Fair objective with Fair Centers 11003.5443
        Il rapporto per K=5,N=11 è 0.24367503894470555

        OR

        NA=7, NB=5 with K=5 and N=12
        Standard centers: [[1199.4678091340274,68.00046615130844], [1998.933861331304,97.42716509073034], [196.26320978548227,65.88382329937855], [999.2015877996207,70.24136781377929], [3.358338659000033,68.16727341427953]]
        Fair centers: [[2000.3554683896775,18.63951295579091], [-1.0850186078373345,19.443547797657967], [1100.5266259618836,20.06923280393979], [198.11524753148538,20.123202700405233], [1998.459992311846,123.68971580237681]]
        Fair objective with Standard Centers 3110.0723
        Fair objective with Fair Centers 8224.5512
        Il rapporto per K=5,N=12 è 0.3781449229794658

        OR

        NA=9, NB=5 with K=5 and N=14
        Standard centers: [[1199.4678091340274,68.00046615130844], [1999.6879218391714,103.10260673991638], [196.26320978548227,65.88382329937855], [999.2015877996207,70.24136781377929], [3.358338659000033,68.16727341427953]]
        Fair centers: [[-1.0912278829447088,19.375459863593377], [1998.6982924733709,20.433255997559773], [1100.5269959018103,20.054008575512597], [7.8079052009447745,116.95908696496569], [198.11582235060064,20.10899992321958]]
        Fair objective with Standard Centers 3244.1375
        Fair objective with Fair Centers 10821.6509
        Il rapporto per K=5,N=14 è 0.2997821252284017

        OR

        NA=8, NB=6 with K=6 and N=14
        Standard centers: [[999.4678091340274,68.00046615130844], [-1.0912278829447088,19.375459863593377], [2098.880334142252,71.78552570311001], [195.85023441479424,93.12185105100461], [1199.7391605470843,69.70927575725331], [7.8079052009447745,116.95908696496569]]
        Fair centers: [[1200.4254928625799,19.44798454990727], [-1.0912278829447088,19.375459863593377], [2099.524322299131,19.47837975236423], [200.43178674781882,19.260947916373198], [1000.6096754295191,21.02611291414021], [7.8079052009447745,116.95908696496569]]
        Fair objective with Standard Centers 5902.7531
        Fair objective with Fair Centers 11508.9891
        Il rapporto per K=6,N=14 è 0.5128819752111068

        //TODO: greater N
        NA=10, NB=4 with K=4 and N=14
        Standard centers: [[1000.007817370985,86.58207270818056], [0.4111608586551938,84.05935483459962], [197.94178018974995,98.59133957054333], [1200.9158513664966,82.51549767745593]]
        Fair centers: [[1098.769085070433,21.57113943718113], [1.175882893110615,116.91819314146629], [200.60613999656528,21.10872544458867], [-1.1182832102556484,18.34167822086628]]
        Fair objective with Standard Centers 4578.4034
        Fair objective with Fair Centers 11414.6490
        Il rapporto per K=4,N=14 è 0.4010989200111129

        OR

        NA=7, NB=5 with K=5 and N=12
        Standard centers: [[2000.3554683896775,18.63951295579091], [3.358338659000033,68.16727341427953], [1099.334698466824,69.12091698254386], [196.26320978548227,65.88382329937855], [1998.459992311846,123.68971580237681]]
        Fair centers: [[198.11524753148538,20.123202700405233], [1998.459992311846,123.68971580237681], [1100.5266259618836,20.06923280393979], [-1.0850186078373345,19.443547797657967], [2000.3554683896775,18.63951295579091]]
        Fair objective with Standard Centers 5873.4562
        Fair objective with Fair Centers 8224.5512
        Il rapporto per K=5,N=12 è 0.7141369916625694

        NA=13, NB=5 with K=5 and N=18
        Standard centers: [[0.4111608586551938,84.05935483459962], [1999.2812953808952,101.6432940950844], [1000.3971287636259,86.61284533663355], [1198.8945413947772,89.88811414025389], [196.33078457015176,86.92633769989567]]
        Fair centers: [[1999.171041933241,20.332482317333053], [-1.1165594397970366,18.415745780352804], [1100.0824236935207,19.401872123038988], [1999.3033460704262,117.90545645063467], [200.44059502600825,19.12069515624353]]
        Fair objective with Standard Centers 5014.4919
        Fair objective with Fair Centers 9426.4754
        Il rapporto per K=5,N=18 è 0.5319583103540181

        NA=10, NB=6 with K=6 and N=16
        Standard centers: [[999.7391605470846,69.70927575725331], [3.358338659000033,68.16727341427953], [2000.4851060755511,67.17446412670662], [197.35291414957857,100.77751482617467], [1197.2755622089537,76.39658727951341], [2198.5648227728234,65.91976040870293]]
        Fair centers: [[1200.351857714202,18.707223476686238], [-1.0912278829447088,19.375459863593377], [2099.1572651714146,19.388805796996028], [200.5999178370858,21.237256362332133], [1000.4256941626563,19.433243000465268], [7.8079052009447745,116.95908696496569]]
        Fair objective with Standard Centers 3187.4087
        Fair objective with Fair Centers 10618.6317
        Il rapporto per K=6,N=16 è 0.300171322876036

        NA=13, NB=6 with K=6 and N=19
        Standard centers: [[1000.8450252791588,88.06815888769047], [0.4111608586551938,84.05935483459962], [2199.597741762649,89.89442903193944], [197.60953403614337,93.99112875117014], [1198.8093423941145,88.72942130308786], [1996.8683253860017,84.21940796921467]]
        Fair centers: [[2197.778275494785,19.445082890775613], [-1.1162149965045856,18.43054593772057], [1100.4399348557151,19.1993382603871], [1999.6148819748935,18.592036496457], [199.66485346387867,19.008749454580787], [196.9244275602316,118.98525518336658]]
        Fair objective with Standard Centers 4810.0810
        Fair objective with Fair Centers 11052.7701
        Il rapporto per K=6,N=19 è 0.43519234605626184

        NA=16, NB=6 with K=6 and N=22
        Standard centers: [[1196.868325386002,84.21940796921469], [-1.1182832102556484,18.34167822086628], [2098.2262365661227,87.92833732109177], [198.80466083393782,105.17199849801071], [998.8093423941145,88.72942130308786], [1.175882893110615,116.91819314146629]]
        Fair centers: [[-1.1166025024876143,18.413895447914353], [1997.7778138834121,19.427209396810156], [1096.739843264513,120.37656989095534], [1100.0368151411485,18.67010412654315], [2200.2322100695783,20.812682291578707], [200.4223474756568,19.593905977057894]]
        Fair objective with Standard Centers 7715.1935
        Fair objective with Fair Centers 10034.3002
        Il rapporto per K=6,N=22 è 0.7688820645384249

        NA=11, NB=8 with K=7 and N=19
        Standard centers: [[1199.4678091340274,68.00046615130844], [2999.066226292557,89.88706461050693], [196.26320978548227,65.88382329937855], [2197.2755622089535,76.39658727951341], [999.2015877996207,70.24136781377929], [3.358338659000033,68.16727341427953], [1999.7391605470843,69.70927575725331]]
        Fair centers: [[1200.6113509486615,20.95718502971296], [2998.4748992495947,20.015065976106957], [98.51330386964904,19.71735767900953], [2100.3883078581257,19.08366368517444], [1198.3242673193934,115.04374727290393], [1000.4434652315408,19.116930753151884], [101.10824457483328,114.33373903464854]]
        Fair objective with Standard Centers 3129.4173
        Fair objective with Fair Centers 10780.7883
        Il rapporto per K=7,N=19 è 0.29027722226338654
         */

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //Spark Configuration that defines the application.
        SparkConf conf = new SparkConf(true).setAppName("G36HW2");
        //Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");

        for (int K = 2; K < 15; K++) {
            for (int N = K+1; N < 2*K+3; N++) {
                //TODO: doing this we can see how to improve the gap in some way
                func(K, N,sc);
            }
            for (int N = 10001; N < 10002; N += 143) {
                func(K,N,sc);
            }
        }
    }

    private static void func(int K, int N, JavaSparkContext sc) {
        List<Pair<double[], Boolean>> dataset = generateDataset(N, K);
        if (dataset.size() != N) {
            System.out.print("------------------\nERROR: size is "+dataset.size());
            System.out.println("--------------------");
        }

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
        System.out.println("NA="+NA+", NB="+NB+" with K="+K+" and N="+N);
        if (NA == 0L || NB == 0L) {
            System.out.println("NA="+NA+", NB="+NB+" with K="+K+" and N="+N);
        }

        Vector[] c_fair = MRFairLloyd(inputPoints, NA, NB, K, 200);

        System.out.println("Standard centers: "+ Arrays.toString(c_stand));
        System.out.println("Fair centers: "+Arrays.toString(c_fair));

        double fair_rad = MRComputeFairObjective(inputPoints, c_fair);

        System.out.printf(Locale.ENGLISH, "Fair objective with Standard Centers %.4f \n", stand_rad);
        System.out.printf(Locale.ENGLISH, "Fair objective with Fair Centers %.4f \n", fair_rad);


        // Division
        double division = stand_rad/fair_rad;
        System.out.println("Il rapporto per K="+K+",N="+N+" è "+division);

        System.out.println("------------------------------------------");
    }
}
