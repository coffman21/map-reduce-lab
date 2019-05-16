package com.banditos.dummymapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MainCounter {

    private static final String INPUT_FILE_PATH = "output/generated.txt";

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();

        FileWorker fileWorker = new FileWorker();
        fileWorker.splitFile(Paths.get(INPUT_FILE_PATH), 8);

    }

}
