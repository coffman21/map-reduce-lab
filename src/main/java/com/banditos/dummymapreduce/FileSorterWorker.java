package com.banditos.dummymapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FileSorterWorker extends AbstractFileWorker {

    public FileSorterWorker(int nThreads, boolean toSplit) {
        super(nThreads, toSplit);
    }

    public List<Path> splitAndSortChunks(Path readFile) throws IOException {
        List<Path> paths = super.splitFile(readFile);
        List<Future<Path>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final Future<Path> future = executor
                    .submit(new MapSorter(paths.get(i)));
            futures.add(future);
        }
        List<Path> newPaths = new ArrayList<>();
        futures.forEach(pathFuture -> {
            try {
                newPaths.add(pathFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        executor.shutdown();
        return newPaths;
    }

    public Path sortFile(List<Path> paths) throws IOException {

        Path newFilePath = Paths.get(paths.get(0).getParent().toString() + "/reduced.txt");
        Files.deleteIfExists(newFilePath);
        Files.createFile(newFilePath);

        List<ReduceSorter> reducers = paths.stream().map(ReduceSorter::new).collect(Collectors.toList());
        BufferedWriter writer = Files
                .newBufferedWriter(newFilePath);


        while (!reducers.isEmpty()) {
            reducers.sort(ReduceSorter::compareTo);
            ReduceSorter minSorter = reducers.get(0);
            String currRow = minSorter.getCurrRowAndIncrement();
            synchronized (this) {
                writer.write(currRow);
                writer.newLine();
            }
            if (minSorter.isDone()) {
                reducers.remove(minSorter);
            }
        }

        return newFilePath;
    }
}
