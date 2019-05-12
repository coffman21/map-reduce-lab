package com.banditos.dummymapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MainGenerator {

    public static void main(String[] args) throws IOException {
        Path path = Paths.get("output/generated.txt");

        for (int i = 0; i < 20; i++) {
            String s = Generator.generateWord();

            Files.write(path, (s + System.lineSeparator()).getBytes(),
                    Files.exists(path)
                            ? StandardOpenOption.APPEND
                            : StandardOpenOption.CREATE);
            System.out.println(s);
            System.out.println(s.length());
        }
    }
}
