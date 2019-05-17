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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public abstract class AbstractFileWorker {

    int nThreads;
    ExecutorService executor;
    boolean toSplit;

    public AbstractFileWorker(int nThreads, boolean toSplit) {
        this.nThreads = nThreads;
        this.executor = Executors.newFixedThreadPool(nThreads);
        this.toSplit = toSplit;
    }

    public List<Path> splitFile(Path readFile) throws IOException {
        String parentPathStr = readFile.getParent().toString();
        Path dirPath = Paths.get(parentPathStr, "splitter");
        if (!toSplit) {
            return Arrays.asList(new File(dirPath.toUri()).listFiles())
                    .stream()
                    .map(file -> Paths.get(file.getPath()))
                    .collect(
                            Collectors.toList());
        }


        for (File f : new File(dirPath.toUri()).listFiles()) {
            f.delete();
        }
        Files.delete(dirPath);
        Files.createDirectory(dirPath);

        BufferedReader bufferedReader = Files
                .newBufferedReader(readFile, Charset.forName("UTF-8"));

        int i = 1;
        long ptr = 0;
        List<Path> paths = new ArrayList<>();
        long fileSize = Files.size(readFile);
        long chunkSize = fileSize / nThreads;

        while (i <= nThreads) {
            Path writeFile = Files.createFile(Paths.get(
                    parentPathStr, "splitter", "split" + ptr + ".txt"));
            paths.add(writeFile);
            BufferedWriter bufferedWriter = Files
                    .newBufferedWriter(writeFile, Charset.forName("UTF-8"));
            do {
                bufferedWriter.write(bufferedReader.read());
            } while (ptr++ <= i * chunkSize);
            i++;
        }
        return paths;
    }


}