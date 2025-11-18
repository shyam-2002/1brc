package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;

public class ResultVerifier {
    private static final String FILE_A = "./results_normal.txt";
    private static final String FILE_B = "./results_produce_consume_v1.txt";

    public static void main(String[] args) throws IOException {
        try (BufferedReader r1 = new BufferedReader(new FileReader(FILE_A));
             BufferedReader r2 = new BufferedReader(new FileReader(FILE_B))) {
            String line1 = null, line2 = null;
            while ((line1 = r1.readLine()) != null && (line2 = r2.readLine()) != null) {
                if (!Objects.equals(line1, line2)) {
                    System.out.printf("diff detected at line1 : %s, line2 : %s\n", line1, line2);
                }
            }
            if (!Objects.equals(line1, line2)) {
                System.out.printf("diff detected at line1 : %s, line2 : %s", line1, line2);
            }
        }
    }

    private static boolean isBlank(String str) {
        return str == null || str.isBlank();
    }
}
