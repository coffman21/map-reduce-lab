package com.banditos.dummymapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ReduceSorter implements Runnable {

    private Path path;

    public ReduceSorter(Path path) {
        this.path = path;
    }

    @Override
    public void run() {

    }
}
