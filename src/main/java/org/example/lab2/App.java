package org.example.lab2;

/**
 * Hello world!
 *
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;

public class App {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Incorrect number of arguments! Expected two arguments.");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileSystem fs = input.getFileSystem(conf);

        if (!fs.exists(input)) {
            System.err.printf("Input file '%s' does not exist!\n", input);
            System.exit(-1);
        }

        if (fs.exists(output)) {
            System.err.printf("Output file '%s' already exists!\n", output);
            System.exit(-1);
        }

        long startTime = System.nanoTime();
        InputStream in = null;
        OutputStream out = null;

        try {
            in = fs.open(input);
            out = fs.create(output);
            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

        long endTime = System.nanoTime();
        long bytesCopied = fs.getFileStatus(output).getLen();

        System.out.printf("Copied %d bytes from '%s' to '%s' in %f seconds\n",
                bytesCopied, input, output, (endTime - startTime) * 1E-9);
    }
}

