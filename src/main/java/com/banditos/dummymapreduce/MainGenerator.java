package com.banditos.dummymapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class MainGenerator {

    // in mb
    private static final long FILE_SIZE = 800;
    private static final byte[] CHARSET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".getBytes();
    private static final String PATH_TO_FILE = "output/generated.txt";

    private static final Random random = new Random();

    public static void main(String[] args) throws IOException {

        long start = System.currentTimeMillis();
        Path path = Paths.get(PATH_TO_FILE);

        Files.deleteIfExists(path);
        Files.createFile(path);
        do {
            int count = 100000;
            byte[] res = new byte[count*255];
            int ptr = 0;
            for (int i = 0; i < count; i++) {
                int length = random.nextInt(155) + 100;
                for (int j = 0; j < length; j++) {
                    res[ptr++] = CHARSET[random.nextInt(CHARSET.length)];
                }
                res[ptr++] = '\n';
            }
            Files.write(path, res, StandardOpenOption.APPEND);
            log(path, start);
        } while (Files.size(path) / (1024 * 1024) <= FILE_SIZE);
        log(path, start);
    }

    private static void log(Path path, long start) throws IOException {
        System.out.println(String.format("Written %d MB for %d millis",
                Files.size(path) / (1024 * 1024), System.currentTimeMillis() - start));
    }
}
