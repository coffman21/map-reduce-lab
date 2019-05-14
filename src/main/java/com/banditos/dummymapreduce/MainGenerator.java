package com.banditos.dummymapreduce;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MainGenerator {

    public static void main(String[] args) throws IOException {
        String pathToFile = "output/generated.txt";
        Path path = Paths.get(pathToFile);

        try {
            FileOutputStream out = new FileOutputStream(pathToFile, true);
            FileChannel channel = out.getChannel();
            // db query
            while(iterate resultset) {
                // get row result
                writeData(channel, data);
            }
        } catch(Exception e){
            //log
        } finally {
            channel.close();
            out .close();
        }

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
