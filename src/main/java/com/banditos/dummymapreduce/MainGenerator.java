package com.banditos.dummymapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class MainGenerator {

    // in mb
    private static final long FILE_SIZE = 800;
    private static final String CHARSET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final String PATH_TO_FILE = "output/generated.txt";

    private static final Random random = new Random();

    public static void main(String[] args) throws IOException {

        long start = System.currentTimeMillis();
        Path path = Paths.get(PATH_TO_FILE);

        Files.deleteIfExists(path);
        Files.createFile(path);
        BufferedWriter writer = Files.newBufferedWriter(path);
        do {
            for (int i = 0; i < 100000; i++) {
                int length = random.nextInt(155) + 100;
                StringBuilder stringBuilder = new StringBuilder();
                for (int j = 0; j < length; j++) {
                    stringBuilder.append(CHARSET.charAt(random.nextInt(CHARSET.length())));
                }
                writer.write(stringBuilder.toString());
                writer.newLine();
            }
            log(path, start);
        } while (Files.size(path) / (1024 * 1024) <= FILE_SIZE);
        log(path, start);
    }

    private static void log(Path path, long start) throws IOException {
        System.out.println(String.format("Written %d MB for %d millis",
                Files.size(path) / (1024 * 1024), System.currentTimeMillis() - start));
    }
}
