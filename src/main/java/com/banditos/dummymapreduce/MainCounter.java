package com.banditos.dummymapreduce;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class MainCounter {

    private static final String INPUT_FILE_PATH = "output/generated.txt";
    private static final String WORD = "M6RvQptghDn8zqTQtCfSNJwOJng2OtBmA4EeHo29qsIXroIJoYo3rAOK3WzvLsarxQRSd8zwmOxrmOY2yWKrhA7t910A2ulgdJcJiFctX2fZya0z";

    public static void main(String[] args) throws IOException {
        FileCounterWorker fileWorker;
        if (args.length >= 1 && args[0].equals("false")) {
            System.out.println("not running splitting");
            fileWorker = new FileCounterWorker(8, false);
        } else {
            fileWorker = new FileCounterWorker(8, true);
        }

        long start = System.currentTimeMillis();

        List<Path> paths = fileWorker.splitFile(Paths.get(INPUT_FILE_PATH));
        int total = fileWorker
                .countWordEntries(paths, WORD);
        System.out.println(total);
        System.out.println(String.format("found %d entries of word %s in %d millis", total, WORD, System.currentTimeMillis() - start));
    }

}
