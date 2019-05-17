package com.banditos.dummymapreduce;

import static java.util.concurrent.CompletableFuture.runAsync;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FileSorterWorker extends AbstractFileWorker{

    public FileSorterWorker(int nThreads, boolean toSplit) {
        super(nThreads, toSplit);
    }

    public List<Path> splitAndSortChunks(Path readFile) throws IOException {
        List<Path> paths = super.splitFile(readFile);

        paths.forEach(path -> executor.execute(new MapSorter(path)));
        executor.shutdown();
        return paths;
    }

    public Path sortFile(List<Path> paths) throws IOException {
        AtomicBoolean flag = new AtomicBoolean(true);

        Path newFilePath = Paths.get(paths.get(0).getParent().toString() + "/reduced.txt");
        Files.deleteIfExists(newFilePath);
        Files.createFile(newFilePath);

        List<ReduceSorter> collect = paths.stream().map(ReduceSorter::new).collect(Collectors.toList());

    }
