package com.banditos.dummymapreduce.count;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class MapCounter implements Callable<Map<String, Integer>> {

    private Path path;

    public MapCounter(Path path) {
        this.path = path;
    }

    @Override
    public Map<String, Integer> call() throws IOException {
        Map<String, Integer> map = new HashMap<>();
        BufferedReader bufferedReader = Files.newBufferedReader(path);
        bufferedReader
                .lines()
                .forEach(s -> map
                        .merge(s, 1, (i,j) -> i+j));

        return map;
    }
}
