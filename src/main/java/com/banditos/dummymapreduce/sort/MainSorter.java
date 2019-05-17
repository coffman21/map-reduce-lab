package com.banditos.dummymapreduce.sort;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class MainSorter {

    private static final String INPUT_FILE_PATH = "output/generated.txt";

    public static void main(String[] args) throws IOException {
        FileSorterWorker fileWorker;
        if (args.length >= 1 && args[0].equals("false")) {
            System.out.println("not running splitting");
            fileWorker = new FileSorterWorker(8, false);
        } else {
            fileWorker = new FileSorterWorker(8, true);
        }

        long start = System.currentTimeMillis();

        List<Path> paths = fileWorker.splitAndSortChunks(Paths.get(INPUT_FILE_PATH));
        Path path = fileWorker.sortFile(paths);
        System.out.println(String.format("file %s sorted in %d millis",
                path.toString(), System.currentTimeMillis() - start));
    }
}
