package Homeworks.Homework_3;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;

public class G36HW3 {

    // After how many items should we stop?
    // public static final int THRESHOLD = 1000000;

    public static final int p = 8191;

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: portExp threshold (T) number_of_rows (D) number_of_cols (W) number_of_top (K)");
        }
        // IMPORTANT: the master must be set to "local[*]" or "local[n]" with n > 1, otherwise
        // there will be no processor running the streaming computation and your
        // code will crash with an out of memory (because the input keeps accumulating).
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("DistinctExample");

        // The definition of the streaming spark context  below, specifies the amount of
        // time used for collecting a batch, hence giving some control on the batch size.
        // Beware that the data generator we are using is very fast, so the suggestion is to
        // use batches of less than a second, otherwise you might exhaust the JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore. The 
        // main thread will first acquire the only permit available, and then it will try
        // to acquire another one right after spinning up the streaming computation.
        // The second attempt at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met the semaphore is released, basically giving "green light" to the main
        // thread to shut down the computation. We cannot call `sc.stop()` directly in `foreachRDD`
        // because it might lead to deadlocks.

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        int portExp = Integer.parseInt(args[0]);
        //System.out.println("Receiving data from port = " + portExp);
        int THRESHOLD = Integer.parseInt(args[1]);
        //System.out.println("Threshold = " + THRESHOLD);
        int D = Integer.parseInt(args[2]);
        //System.out.println("Rows = " + THRESHOLD);
        int W = Integer.parseInt(args[3]);
        //System.out.println("Columns = " + THRESHOLD);
        int K = Integer.parseInt(args[4]);
        //System.out.println("Top elements = " + THRESHOLD);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Variable streamLength below is used to maintain the number of processed stream items.
        // It must be defined as a 1-element array so that the value stored into the array can be
        // changed within the lambda used in foreachRDD. Using a simple external counter streamLength of type
        // long would not work since the lambda would not be allowed to update it.
        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0]=0L;

        // Hash Table for the distinct elements
        HashMap<Long, Long> histogram = new HashMap<>();

        // Create the hash functions for both CM and CS
        ArrayList<int[]> CM_h = new ArrayList<>(D);
        ArrayList<int[]> CS_h = new ArrayList<>(D);
        ArrayList<int[]> CS_g = new ArrayList<>(D);

        Random random = new Random(1);
        for (int i = 0; i<D; i++) {
            CM_h.add(generateHashFunction(random));
            CS_h.add(generateHashFunction(random));
            CS_g.add(generateHashFunction(random));
        }

        long[][] counter_CM = new long[D][W];
        long[][] counter_CS = new long[D][W];
        PriorityQueue<Pair<Long, Long>> topKHeap = new PriorityQueue<>(K, Comparator.comparingLong(Pair::getValue));

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
			// For each batch, to the following.
			// BEWARE: the `foreachRDD` method has "at least once semantics", meaning
			// that the same data might be processed multiple times in case of failure.
			.foreachRDD((batch, time) -> {
				// this is working on the batch at time `time`.
				if (streamLength[0] >= THRESHOLD) return;
				long batchSize = batch.count();
				streamLength[0] += batchSize;
				if (batchSize == 0) return;
				//System.out.println("Batch size at time [" + time + "] is: " + batchSize);
				// Extract the distinct items from the batch
				Map<Long, Long> batchItems = batch
						.mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
						.reduceByKey((i1, i2) -> i1 + i2)
						.collectAsMap();
				// Update the streaming state. If the overall count of processed items reaches the
				// THRESHOLD value (among all batches processed so far), subsequent items of the
				// current batch are ignored, and no further batches will be processed
				for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
					long x = pair.getKey();
					long count = pair.getValue();

					// Exact frequencies
					histogram.put(x, histogram.getOrDefault(x, 0L) + count);

                    // Update the top-K Heavy Hitters
                    Pair<Long, Long> current = new ImmutablePair<>(x, histogram.get(x));
                    if (topKHeap.contains(current)) {
                        topKHeap.remove(current);
                        topKHeap.add(current);
                    } else if (topKHeap.size() < K) {
                        topKHeap.add(current);
                    } else if (topKHeap.peek().getValue() < current.getValue()) {
                        topKHeap.poll();
                        topKHeap.add(current);
                    }

					// Count-min sketch
					for (int j = 0; j < D; j++) {
						int i = hash(x, W, CM_h.get(j));
						counter_CM[j][i] += count;

						i = hash(x, W, CS_h.get(j));
						int sign = hashSign(x, CS_g.get(j));
						counter_CS[j][i] += sign * count;
					}
				}

				// If we wanted, here we could run some additional code on the global histogram
				if (streamLength[0] >= THRESHOLD) {
					// Stop receiving and processing further batches
					stoppingSemaphore.release();
				}

			});

        // MANAGING STREAMING SPARK CONTEXT
        //System.out.println("Starting streaming engine");
        sc.start();
        //System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        //System.out.println("Stopping the streaming engine");

        /* The following command stops the execution of the stream. The first boolean, if true, also
           stops the SparkContext, while the second boolean, if true, stops gracefully by waiting for
           the processing of all received data to be completed. You might get some error messages when
           the program ends, but they will not affect the correctness. You may also try to set the second
           parameter to true.
        */

        sc.stop(false, true);
        //System.out.println("Streaming engine stopped");

        // COMPUTE AND PRINT FINAL STATISTICS
        //System.out.println("Number of items processed = " + streamLength[0]);
        //System.out.println("Number of distinct items = " + histogram.size());
        /* SORTING THE TOP-K HEAVY HITTERS AFTER THE STREAMING PROCESSING
		ArrayList<Pair<Long, Long>> topK = new ArrayList<>();
		for (Map.Entry<Long, Long> entry : histogram.entrySet()) {
			topK.add(new ImmutablePair<>(entry.getKey(), entry.getValue()));
		}
		Collections.sort(topK, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        */
        ArrayList<Pair<Long, Long>> topK = new ArrayList<>(topKHeap);
        topK.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        long phi_K = topK.get(Math.min(K, topK.size())-1).getValue();
		//System.out.println("Phi(K) = " + phi_K);
		topK.removeIf(e -> e.getValue() < phi_K);

        topK.sort(Map.Entry.comparingByKey());

        double sumRelErrCM = 0.0;
        double sumRelErrCS = 0.0;
        for (Pair<Long, Long> pair : topK) {
            long trueFreq = pair.getValue();
            long estCM = estimateFrequencyCM(pair.getKey(), W, D, CM_h, counter_CM);
            long estCS = estimateFrequencyCS(pair.getKey(), W, D, CS_h, CS_g, counter_CS);
            sumRelErrCM += Math.abs(trueFreq - estCM) / (double) trueFreq;
            sumRelErrCS += Math.abs(trueFreq - estCS) / (double) trueFreq;
        }
        double avgRelErrCM = sumRelErrCM / topK.size();
        double avgRelErrCS = sumRelErrCS / topK.size();

        System.out.printf("Port = %d T = %d D = %d W = %d K = %d\n", portExp, THRESHOLD, D, W, K);
        System.out.printf("Number of processed items = %d\n", streamLength[0]);
        System.out.printf("Number of distinct items  = %d\n", histogram.size());
        System.out.printf("Number of Top-K Heavy Hitters = %d\n", topK.size());
        System.out.printf(Locale.US, "Avg Relative Error for Top-K Heavy Hitters with CM = %.15f\n", avgRelErrCM);
        System.out.printf(Locale.US, "Avg Relative Error for Top-K Heavy Hitters with CS = %.15f\n", avgRelErrCS);

        if (K <= 10) {
            System.out.println("Top-K Heavy Hitters:");
            for (Pair<Long, Long> pair : topK) {
                long cm_freq = estimateFrequencyCM(pair.getKey(), W, D, CM_h, counter_CM);
                System.out.printf("Item %d True Frequency = %d Estimated Frequency with CM = %d\n",
                        pair.getKey(), pair.getValue(), cm_freq);
            }
        }

        /* USELESS for our purposes:

        ArrayList<Long> distinctKeys = new ArrayList<>(histogram.keySet());
        Collections.sort(distinctKeys, Collections.reverseOrder());
        System.out.println("Largest item = " + distinctKeys.get(0));
         */
    }

    // Estimates the frequency of the item x with CM
    private static long estimateFrequencyCM(long x, int W, int D, ArrayList<int[]> CM_h, long[][] counter_CM) {
        long min = Long.MAX_VALUE;
        for (int j = 0; j < D; j++) {
            min = Math.min(min, counter_CM[j][hash(x, W, CM_h.get(j))]);
        }
        return min;
    }

    private static long estimateFrequencyCS(long x, int W, int D, ArrayList<int[]> CS_h, ArrayList<int[]> CS_g, long[][] counter_CS) {
		ArrayList<Long> values = new ArrayList<>();
        for (int j = 0; j < D; j++) {
			int i = hash(x, W, CS_h.get(j));
			int sign = hashSign(x, CS_g.get(j));
            long v = counter_CS[j][i] * sign;
			values.add(v);
        }
		Collections.sort(values);
		return D % 2 == 0
			? (values.get(D / 2 - 1) + values.get(D / 2)) / 2
			: values.get(D / 2);
    }

    // a is the first element of the hash function, b the second
    private static int hash(long x, int C, int[] hashFunc) {
        int a = hashFunc[0];
        int b = hashFunc[1];
        return ((int)((a*x+b)%p))%C;
    }

    private static int hashSign(long x, int[] hashFunc) {
		return hash(x, 2, hashFunc) * 2 - 1;
    }

    private static int[] generateHashFunction(Random random) {
        int a = random.nextInt(p-1)+1;
        int b = random.nextInt(p);
        return new int[] {a,b};
    }

}
