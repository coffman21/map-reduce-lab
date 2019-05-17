package com.banditos.dummymapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MapSorter implements Runnable {

    private final Path path;

    MapSorter(Path path) {
        this.path = path;
    }

    @Override
    public void run() {
        try {
            BufferedReader bufferedReader = Files
                    .newBufferedReader(path, Charset.forName("UTF-8"));
            Path newFilePath = Paths.get(path.toString() + ".sorted.txt");

            Files.deleteIfExists(newFilePath);
            Files.createFile(newFilePath);
            BufferedWriter writer = Files
                    .newBufferedWriter(newFilePath, Charset.forName("UTF-8"));
            bufferedReader.lines().sorted().forEach(s -> {
                try {
                    writer.write(s);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
