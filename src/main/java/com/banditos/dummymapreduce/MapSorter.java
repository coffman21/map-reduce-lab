package com.banditos.dummymapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

public class MapSorter implements Callable<Path> {

    private final Path path;

    MapSorter(Path path) {
        this.path = path;
    }

    @Override
    public Path call() {
        try {
            BufferedReader bufferedReader = Files
                    .newBufferedReader(path);
            Path newFilePath = Paths.get(path.toString() + ".sorted.txt");

            Files.deleteIfExists(newFilePath);
            Files.createFile(newFilePath);
            BufferedWriter writer = Files
                    .newBufferedWriter(newFilePath);
            bufferedReader.lines().sorted().forEach(s -> {
                try {
                    writer.write(s);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            return newFilePath;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
