package dev.morling.onebrc;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CalculateAverage_shyam_goyal_pc_v1 {
    private static final String INPUT_FILE_NAME = "./measurements.txt";
    private static final String OUTPUT_FILE_NAME = "./results_produce_consume_v1.txt";
    private static final int PROCESSING_BATCH_SIZE = 1000;

    private AtomicLong totalProcessedBatches = new AtomicLong();
    private AtomicReference<Boolean> reading = new AtomicReference<>(true);
    private volatile boolean consuming = false;

    private volatile Queue<List<String>> q = new LinkedList<>();
    private final Map<String, Result> resultMap = new HashMap<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        new CalculateAverage_shyam_goyal_pc_v1().doWork();
    }

    private void doWork() throws IOException {
        long start = System.currentTimeMillis();
        try (BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE_NAME))) {
            List<String> lst = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                while (consuming && (q.size() > 10)) {
                    Thread.sleep(10);
                }
                lst.add(line);
                if (lst.size() == PROCESSING_BATCH_SIZE) {
                    startQueuePoller();
                    q.add(new ArrayList<>(lst));
                    lst.clear();
                }
            }
            q.add(new ArrayList<>(lst));
            lst.clear();
            reading.set(false);
            consuming = false;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.printf("time taken in processing : millis : %s\n", (end - start));
        outputResults();
    }

    private void startQueuePoller() {
        if (!consuming) {
            Thread t = new Thread(() -> {
                while (!q.isEmpty() || reading.get()) {
                    List<String> batch = q.poll();
                    if (batch != null) {
                        processBatch(batch);
                        totalProcessedBatches.incrementAndGet();
                    }
                }
            });
            t.start();
            consuming = true;
            startStatsPrinter();
        }
    }


    private void startStatsPrinter() {
        new Thread(() -> {
            while (consuming) {
                System.out.printf("queue size : %s, totalProcessedBatches : %s\n", q.size(), totalProcessedBatches.get());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }


    private void processBatch(List<String> batch) {
        batch.forEach(this::processStr);
    }

    private void processStr(String str) {
        if (str != null && !str.isEmpty()) {
            String[] split = str.split(";");
            String location = split[0];
            double value = Double.parseDouble(split[1]);
            if (resultMap.containsKey(location)) {
                resultMap.get(location).update(value);
            } else {
                resultMap.put(location, new Result(value, value, value));
            }
        }
    }

    private void outputResults() {
        long start = System.currentTimeMillis();
        System.out.print("start of outputResults");
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
            mean = ((mean * measurementsCnt) + val) / (measurementsCnt + 1);
            measurementsCnt++;
        }

        public void update(double measurement) {
            updateMin(measurement);
            updateMax(measurement);
            updateMean(measurement);
        }
    }
}
