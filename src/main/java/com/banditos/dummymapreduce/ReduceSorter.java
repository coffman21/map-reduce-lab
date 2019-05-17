package com.banditos.dummymapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public class ReduceSorter implements Comparable<ReduceSorter>{

    private Path path;
    private BufferedReader bufferedReader;
    private String currRow;

    public ReduceSorter(Path path) {
        try {
            this.path = path;
            this.bufferedReader = Files
                    .newBufferedReader(path);

            this.currRow = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String getCurrRow() {
        return currRow;
    }


    public String getCurrRowAndIncrement() {
        try {
            String s = currRow;
            currRow = bufferedReader.readLine();
            return s;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean isDone() {
        return currRow == null;
    }

    @Override
    public int compareTo(ReduceSorter o) {
        String thatCurrRow = o.getCurrRow();
        return this.getCurrRow().compareTo(thatCurrRow);
    }
}
