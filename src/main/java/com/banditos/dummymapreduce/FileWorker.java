package com.banditos.dummymapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class FileWorker {

    Executor executor = Executors.newWorkStealingPool();

    public List<Path> splitFile(Path readFile, int splitAmount) throws IOException {
        long fileSize = Files.size(readFile);
        long chunkSize = fileSize / splitAmount;

        String parentPathStr = readFile.getParent().toString();
        Path dirPath = Paths.get(parentPathStr, "splitter");

        for (File f : new File(dirPath.toUri()).listFiles()) {
            f.delete();
        }
        Files.delete(dirPath);
        Files.createDirectory(dirPath);

        InputStream reader = Files.newInputStream(readFile);

        int i = 1;
        long ptr = 0;

        List<Path> paths = new ArrayList<>();

        while (i < splitAmount) {
            Path writeFile = Files.createFile(Paths.get(parentPathStr, "splitter", "split" + ptr));
            paths.add(writeFile);
            OutputStream outputStream = Files.newOutputStream(writeFile);

            do {
                outputStream.write(reader.read());
            } while (ptr++ <= i * chunkSize);

            i++;
        }
        return paths;
    }
}
