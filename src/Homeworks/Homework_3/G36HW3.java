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

    // Prime number used in the following
    public static final int p = 8191;

    // Hostname
    public static final String hostname = "algo.dei.unipd.it";

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: portExp threshold (T) number_of_rows (D) number_of_cols (W) number_of_top (K)");
        }

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
        int THRESHOLD = Integer.parseInt(args[1]);
        int D = Integer.parseInt(args[2]);
        int W = Integer.parseInt(args[3]);
        int K = Integer.parseInt(args[4]);
        System.out.printf("Port = %d T = %d D = %d W = %d K = %d\n", portExp, THRESHOLD, D, W, K);

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

        // D functions of each type
        Random random = new Random(1);
        for (int i = 0; i<D; i++) {
            CM_h.add(generateHashFunction(random));
            CS_h.add(generateHashFunction(random));
            CS_g.add(generateHashFunction(random));
        }

        // Tables with counters
        long[][] counter_CM = new long[D][W];
        long[][] counter_CS = new long[D][W];

        // Heap with top K hitters so far (to not compute them only at the end of the streaming, we keep them updated in a streaming fashion)
        // This is done to reduce complexity to O(NlogN) to O(NlogK), which is a great improvement considering that K is a lot smaller than N.
        PriorityQueue<Pair<Long, Long>> topKHeap = new PriorityQueue<>(K, Comparator.comparingLong(Pair::getValue));

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PROCESS THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        sc.socketTextStream(hostname, portExp, StorageLevels.MEMORY_AND_DISK)
			// For each batch, do the following.
			// BEWARE: the `foreachRDD` method has "at least once semantics", meaning
			// that the same data might be processed multiple times in case of failure.
			.foreachRDD((batch, time) -> {
				// this is working on the batch at time `time`.
				if (streamLength[0] >= THRESHOLD) return;
				long batchSize = batch.count();
				streamLength[0] += batchSize;
				if (batchSize == 0) return;

				// Extract the distinct items from the batch
				Map<Long, Long> batchItems = batch
						.mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
						.reduceByKey(Long::sum)
						.collectAsMap();

				// Update the streaming state. If the overall count of processed items reaches the
				// THRESHOLD value (among all batches processed so far), subsequent items of the
				// current batch are ignored, and no further batches will be processed
				for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
					long x = pair.getKey();
					long count = pair.getValue();

                    // Exact frequencies
                    long prev_freq = histogram.getOrDefault(x,0L);
                    histogram.put(x, prev_freq + count);
                    long actualFreq = histogram.get(x);

                    // Maintain top K heavy hitters
                    Pair<Long, Long> newPair = new ImmutablePair<>(x, actualFreq);
                    Pair<Long, Long> oldPair = new ImmutablePair<>(x, prev_freq);

                    // First, remove the old pair if it exists (for updates)
                    topKHeap.remove(oldPair);

                    // Now decide whether to add the new pair
                    if (topKHeap.size() < K) {
                        topKHeap.add(newPair);
                    } else {
                        // Smallest frequency in heap
                        long minFreqInHeap = topKHeap.peek().getValue();

                        if (newPair.getValue() > minFreqInHeap) {
                            topKHeap.add(newPair);
                            // Remove excess elements
                            ArrayList<Pair<Long,Long>> removed = new ArrayList<>();
                            while (topKHeap.size() > K && topKHeap.peek().getValue() == minFreqInHeap) {
                                removed.add(topKHeap.poll());
                            }
                            if (topKHeap.size() <= K && topKHeap.peek().getValue() == minFreqInHeap) {
                                topKHeap.addAll(removed);
                            }
                        } else if (newPair.getValue().equals(minFreqInHeap)) {
                            topKHeap.add(newPair);
                        }
                    }

					// Count-Min sketch and Count Sketch
					for (int j = 0; j < D; j++) {
						int i = hash(x, W, CM_h.get(j));
						counter_CM[j][i] += count;

						i = hash(x, W, CS_h.get(j));
						int sign = hashSign(x, CS_g.get(j));
						counter_CS[j][i] += sign * count;
					}
				}

				if (streamLength[0] >= THRESHOLD) {
					// Stop receiving and processing further batches
					stoppingSemaphore.release();
				}

			});

        // MANAGING STREAMING SPARK CONTEXT
        sc.start();
        stoppingSemaphore.acquire();

        /* The following command stops the execution of the stream. The first boolean, if true, also
           stops the SparkContext, while the second boolean, if true, stops gracefully by waiting for
           the processing of all received data to be completed. You might get some error messages when
           the program ends, but they will not affect the correctness. You may also try to set the second
           parameter to true.
        */

        sc.stop(false, true);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // COMPUTE AND PRINT FINAL STATISTICS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        ArrayList<Pair<Long, Long>> topK = new ArrayList<>(topKHeap);
        // Put them in the right order
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
        long result = (a * x + b) % p;
        return (int)(result % C);
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
