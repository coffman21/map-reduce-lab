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
                .newBufferedReader(readFile);

        int i = 1;
        List<Path> paths = new ArrayList<>();

        List<BufferedWriter> writers = new ArrayList<>();
        while (i <= nThreads) {
            Path writeFile = Files.createFile(Paths.get(
                    parentPathStr, "splitter", "split" + i + ".txt"));
            paths.add(writeFile);
            BufferedWriter bufferedWriter = Files
                    .newBufferedWriter(writeFile);
            writers.add(bufferedWriter);
            i++;
        }

        while (true) {
            for (BufferedWriter writer : writers) {
                String s = bufferedReader.readLine();
                if (s == null) {
                    return paths;
                } else {
                    writer.write(s);
                    writer.newLine();
                }
            }
        }
    }


}
