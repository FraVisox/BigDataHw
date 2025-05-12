package Homeworks.Homework_2;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

import static Homeworks.Homework_2.G36GEN.generateDataset;

public class TestGEN {
    public static void main(String[] args) { //TODO: test
        for (int K = 1; K < 1000; K++) {
            for (int N = K; N < 100000; N++) {
                List<Pair<double[], Boolean>> dataset = generateDataset(N, K);
                if (dataset.size() != N) {
                    System.out.println("ERROREEEEEEEEEEEEEE:");
                    System.out.println(dataset.size());
                }
                if (N % 1000 == 0) {
                    System.out.println("N = "+N);
                }
            }
            System.out.println("K = "+K);
        }
    }
}
