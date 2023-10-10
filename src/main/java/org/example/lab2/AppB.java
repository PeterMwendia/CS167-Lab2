package org.example.lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Random;

public class AppB {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Incorrect number of arguments! Expected one argument.");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(args[0]);

        if (!fs.exists(filePath)) {
            System.err.printf("File '%s' does not exist!\n", filePath);
            System.exit(-1);
        }

        FSDataInputStream in = null;
        try {
            in = fs.open(filePath);
            long fileLength = fs.getFileStatus(filePath).getLen();
            Random rand = new Random();
            byte[] buffer = new byte[8192];

            long startTime = System.nanoTime();
            for (int i = 0; i < 10000; i++) {
                long randomPosition = (long) (rand.nextDouble() * fileLength);
                in.seek(randomPosition);
                in.read(buffer, 0, buffer.length);
            }
            long endTime = System.nanoTime();

            System.out.printf("Finished 10,000 reads in %f seconds\n", (endTime - startTime) * 1E-9);

        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
