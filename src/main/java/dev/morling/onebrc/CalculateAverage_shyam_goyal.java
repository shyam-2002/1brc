package dev.morling.onebrc;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class CalculateAverage_shyam_goyal {
    private static final String INPUT_FILE_NAME = "./measurements.txt";
    private static final String OUTPUT_FILE_NAME = "./results_normal.txt";

    private static final Map<String, Result> resultMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        try (BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE_NAME))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.isEmpty()) {
                    String[] split = line.split(";");
                    String location = split[0];
                    double value = Double.parseDouble(split[1]);
                    if (resultMap.containsKey(location)) {
                        resultMap.get(location).update(value);
                    } else {
                        resultMap.put(location, new Result(value, value, value));
                    }
                }
            }
        }
        long end = System.currentTimeMillis();
        System.out.printf("time taken in processing : millis : %s\n", (end - start));
        outputResults();
    }

    private static void outputResults() {
        long start = System.currentTimeMillis();
        System.out.println("start of outputResults");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(OUTPUT_FILE_NAME))) {
            resultMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
                try {
                    bw.write(String.format("%s=%.1f/%.1f/%.1f\n", e.getKey(), e.getValue().min, e.getValue().mean, e.getValue().max));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.printf("time taken in outputting results : %s\n", (end - start));
    }

    private static class Result {
        private double min;
        private double max;
        private double mean;
        private long measurementsCnt;

        public Result(double min, double max, double mean) {
            this.min = min;
            this.max = max;
            this.mean = mean;
        }

        public void updateMin(double candidate) {
            if (candidate < min) {
                min = candidate;
            }
        }

        public void updateMax(double candidate) {
            if (candidate > max) {
                max = candidate;
            }
        }

        public void updateMean(double val) {
            mean = ((mean * measurementsCnt) + val) / (measurementsCnt+1);
            measurementsCnt++;
        }

        public void update(double measurement) {
            updateMin(measurement);
            updateMax(measurement);
            updateMean(measurement);
        }
    }
}
