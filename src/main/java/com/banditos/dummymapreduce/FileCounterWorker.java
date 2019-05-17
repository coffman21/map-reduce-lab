package com.banditos.dummymapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class FileCounterWorker extends AbstractFileWorker{


    public FileCounterWorker(int nThreads, boolean toSplit) {
        super(nThreads, toSplit);
    }

    @Override
    public List<Path> splitFile(Path readFile) throws IOException {
        return super.splitFile(readFile);
    }

    public int countWordEntries(List<Path> files, String word) {
        List<Future<Map<String, Integer>>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final Future<Map<String, Integer>> future = executor
                    .submit(new MapCounter(files.get(i)));
            futures.add(future);
        }
        int total = 0;
        for (Future<Map<String, Integer>> f : futures) {
            try {
                total += f.get().getOrDefault(word, 0);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();
        return total;
    }
}
